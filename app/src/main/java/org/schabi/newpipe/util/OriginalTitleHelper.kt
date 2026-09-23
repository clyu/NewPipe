/*
 * SPDX-FileCopyrightText: 2026 NewPipe contributors <https://newpipe.net>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.util

import android.util.AtomicFile
import android.util.Log
import com.grack.nanojson.JsonArray
import com.grack.nanojson.JsonParser
import com.grack.nanojson.JsonParserException
import com.grack.nanojson.JsonWriter
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.schabi.newpipe.App
import org.schabi.newpipe.DownloaderImpl
import org.schabi.newpipe.extractor.InfoItem
import org.schabi.newpipe.extractor.ServiceList
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException
import org.schabi.newpipe.extractor.stream.StreamInfoItem

/**
 * In lists such as channel tabs, YouTube returns video titles translated into the content
 * language (by the uploader or automatically), while the video page shows the original title.
 * This helper replaces the titles of YouTube streams in such lists with their original titles,
 * which are taken from YouTube's oEmbed endpoint, as it does not translate them.
 *
 * Original titles are cached on disk along with the hash of the title they replace, so a video
 * is only looked up again once YouTube lists it with another title, e.g. because the uploader
 * renamed it or the content language changed.
 */
object OriginalTitleHelper {
    private val TAG = OriginalTitleHelper::class.java.simpleName

    private const val OEMBED_URL = "https://www.youtube.com/oembed?format=json" +
        "&url=https%3A%2F%2Fwww.youtube.com%2Fwatch%3Fv%3D"
    private const val CACHE_FILE_NAME = "youtube_original_titles.json"
    private const val MAX_CACHE_ENTRIES = 10_000
    private const val LOOKUP_THREADS = 6
    private const val SAVE_DELAY_SECONDS = 10L
    private val PAUSE_AFTER_RATE_LIMIT_MILLIS = TimeUnit.MINUTES.toMillis(10)

    /** Cached instead of an original title when YouTube does not provide one. */
    private const val NO_TITLE = ""

    private class Entry(val listedTitleHash: Int, val originalTitle: String)

    // access ordered, so that the least recently used entries are dropped first
    private val cache = object : LinkedHashMap<String, Entry>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Entry>?) =
            size > MAX_CACHE_ENTRIES
    }
    private var isCacheLoaded = false
    private val isSaveScheduled = AtomicBoolean()
    private val cacheFile by lazy { AtomicFile(File(App.instance.cacheDir, CACHE_FILE_NAME)) }

    private val lookupExecutor = Executors.newFixedThreadPool(LOOKUP_THREADS)

    @Volatile
    private var pausedUntilMillis = 0L

    /**
     * Looks up the titles which are not cached yet over the network, so this must not be called
     * on the main thread.
     *
     * @return [items], where YouTube streams listed with a title other than their original one
     * are replaced by copies with their original title
     */
    @JvmStatic
    fun restoreOriginalTitles(items: List<InfoItem>): List<InfoItem> {
        val streams = items.filterIsInstance<StreamInfoItem>()
            .filter { it.serviceId == ServiceList.YouTube.serviceId }
        if (streams.isEmpty()) {
            return items
        }

        val originalTitles = getOriginalTitles(streams)
        return items.map { item ->
            val originalTitle = originalTitles[item.url]
            if (item is StreamInfoItem && originalTitle != null && originalTitle != item.name) {
                copyWithName(item, originalTitle)
            } else {
                item
            }
        }
    }

    /**
     * @return the original titles of [streams] by their URL, leaving out the unknown ones
     */
    private fun getOriginalTitles(streams: List<StreamInfoItem>): Map<String, String> {
        val originalTitles = HashMap<String, String>()
        val toLookUp = ArrayList<Pair<StreamInfoItem, String>>()

        synchronized(cache) {
            loadCacheIfNeeded()
            for (stream in streams) {
                val videoId = getVideoId(stream.url) ?: continue
                val name = stream.name.orEmpty()
                val entry = cache[videoId]
                // items whose title has already been restored need no lookup either
                if (entry == null ||
                    (entry.listedTitleHash != name.hashCode() && entry.originalTitle != name)
                ) {
                    toLookUp.add(stream to videoId)
                } else if (entry.originalTitle != NO_TITLE) {
                    originalTitles[stream.url] = entry.originalTitle
                }
            }
        }

        if (toLookUp.isEmpty() || isPaused()) {
            return originalTitles
        }

        val lookups = try {
            lookupExecutor.invokeAll(
                toLookUp.map { (_, videoId) -> Callable { fetchOriginalTitle(videoId) } }
            )
        } catch (e: InterruptedException) {
            // the caller is not interested in the result anymore
            Thread.currentThread().interrupt()
            return originalTitles
        }

        var hasNewEntries = false
        synchronized(cache) {
            toLookUp.zip(lookups).forEach { (streamAndVideoId, lookup) ->
                val (stream, videoId) = streamAndVideoId
                val originalTitle = runCatching { lookup.get() }.getOrNull() ?: return@forEach
                cache[videoId] = Entry(stream.name.orEmpty().hashCode(), originalTitle)
                hasNewEntries = true
                if (originalTitle != NO_TITLE) {
                    originalTitles[stream.url] = originalTitle
                }
            }
        }
        if (hasNewEntries) {
            scheduleCacheSave()
        }
        return originalTitles
    }

    /**
     * @return the original title of the video, [NO_TITLE] if YouTube does not provide it (e.g.
     * because the video is private or cannot be embedded), or `null` if it cannot be fetched
     * at the moment
     */
    private fun fetchOriginalTitle(videoId: String): String? {
        if (isPaused()) {
            return null
        }

        return try {
            val response = DownloaderImpl.getInstance().get(OEMBED_URL + videoId)
            when (response.responseCode()) {
                200 -> JsonParser.`object`()
                    .from(response.responseBody())
                    .getString("title", NO_TITLE)
                in 400..499 -> NO_TITLE
                else -> null
            }
        } catch (e: ReCaptchaException) {
            // YouTube is rate limiting us, so leave it alone for a while
            pausedUntilMillis = System.currentTimeMillis() + PAUSE_AFTER_RATE_LIMIT_MILLIS
            null
        } catch (e: IOException) {
            null
        } catch (e: JsonParserException) {
            null
        }
    }

    private fun isPaused() = System.currentTimeMillis() < pausedUntilMillis

    private fun getVideoId(url: String): String? =
        runCatching { ServiceList.YouTube.streamLHFactory.getId(url) }.getOrNull()

    private fun copyWithName(item: StreamInfoItem, name: String) =
        StreamInfoItem(item.serviceId, item.url, name, item.streamType).apply {
            thumbnails = item.thumbnails
            uploaderName = item.uploaderName
            uploaderUrl = item.uploaderUrl
            uploaderAvatars = item.uploaderAvatars
            isUploaderVerified = item.isUploaderVerified
            shortDescription = item.shortDescription
            textualUploadDate = item.textualUploadDate
            uploadDate = item.uploadDate
            viewCount = item.viewCount
            duration = item.duration
            isShortFormContent = item.isShortFormContent
            contentAvailability = item.contentAvailability
        }

    /** Must be called while holding the lock on [cache]. */
    private fun loadCacheIfNeeded() {
        if (isCacheLoaded) {
            return
        }
        isCacheLoaded = true

        try {
            val savedEntries = cacheFile.openRead().use { JsonParser.array().from(it) }
            // entries are saved from the least to the most recently used one, see saveCache()
            for (savedEntry in savedEntries.filterIsInstance<JsonArray>()) {
                val videoId = savedEntry.getString(0)
                val originalTitle = savedEntry.getString(2)
                if (videoId != null && originalTitle != null) {
                    cache[videoId] = Entry(savedEntry.getInt(1), originalTitle)
                }
            }
        } catch (e: FileNotFoundException) {
            // nothing has been cached yet
        } catch (e: IOException) {
            Log.w(TAG, "Could not load the cached original titles", e)
        } catch (e: JsonParserException) {
            Log.w(TAG, "Could not load the cached original titles", e)
        }
    }

    private fun scheduleCacheSave() {
        if (isSaveScheduled.compareAndSet(false, true)) {
            Schedulers.io().scheduleDirect(::saveCache, SAVE_DELAY_SECONDS, TimeUnit.SECONDS)
        }
    }

    @Synchronized
    private fun saveCache() {
        isSaveScheduled.set(false)
        val json = synchronized(cache) {
            val writer = JsonWriter.string().array()
            // iterating does not count as access, so this keeps the least recently used order
            for ((videoId, entry) in cache) {
                writer.array()
                    .value(videoId)
                    .value(entry.listedTitleHash)
                    .value(entry.originalTitle)
                    .end()
            }
            writer.end().done()
        }

        val stream = try {
            cacheFile.startWrite()
        } catch (e: IOException) {
            Log.w(TAG, "Could not save the original titles", e)
            return
        }
        try {
            stream.write(json.toByteArray())
            cacheFile.finishWrite(stream)
        } catch (e: IOException) {
            cacheFile.failWrite(stream)
            Log.w(TAG, "Could not save the original titles", e)
        }
    }
}
