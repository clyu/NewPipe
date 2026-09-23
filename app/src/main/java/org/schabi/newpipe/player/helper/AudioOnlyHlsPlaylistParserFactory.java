package org.schabi.newpipe.player.helper;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.Format;
import com.google.android.exoplayer2.offline.StreamKey;
import com.google.android.exoplayer2.source.hls.playlist.DefaultHlsPlaylistParserFactory;
import com.google.android.exoplayer2.source.hls.playlist.HlsMediaPlaylist;
import com.google.android.exoplayer2.source.hls.playlist.HlsMultivariantPlaylist;
import com.google.android.exoplayer2.source.hls.playlist.HlsPlaylist;
import com.google.android.exoplayer2.source.hls.playlist.HlsPlaylistParserFactory;
import com.google.android.exoplayer2.upstream.ParsingLoadable;
import com.google.android.exoplayer2.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link HlsPlaylistParserFactory} for playing only the audio of HLS streams whose variants
 * contain both audio and video, such as YouTube livestreams.
 *
 * <p>
 * ExoPlayer fetches the whole variants of such streams even if the video is disabled, so this
 * factory keeps only the cheapest variant of multivariant playlists: the one with the lowest
 * bitrate whose audio is not encoded with HE-AAC, which YouTube uses for the low quality audio of
 * its lowest variants, or else the one with the lowest bitrate. Multivariant playlists without
 * video variants are left as-is.
 * </p>
 */
public final class AudioOnlyHlsPlaylistParserFactory implements HlsPlaylistParserFactory {

    private final HlsPlaylistParserFactory parserFactory = new DefaultHlsPlaylistParserFactory();

    @NonNull
    @Override
    public ParsingLoadable.Parser<HlsPlaylist> createPlaylistParser() {
        return keepingCheapestVariant(parserFactory.createPlaylistParser());
    }

    @NonNull
    @Override
    public ParsingLoadable.Parser<HlsPlaylist> createPlaylistParser(
            @NonNull final HlsMultivariantPlaylist multivariantPlaylist,
            @Nullable final HlsMediaPlaylist previousMediaPlaylist) {
        return keepingCheapestVariant(
                parserFactory.createPlaylistParser(multivariantPlaylist, previousMediaPlaylist));
    }

    private static ParsingLoadable.Parser<HlsPlaylist> keepingCheapestVariant(
            final ParsingLoadable.Parser<HlsPlaylist> parser) {
        return (uri, inputStream) -> {
            final HlsPlaylist playlist = parser.parse(uri, inputStream);
            return playlist instanceof HlsMultivariantPlaylist
                    ? keepCheapestVariant((HlsMultivariantPlaylist) playlist)
                    : playlist;
        };
    }

    private static HlsMultivariantPlaylist keepCheapestVariant(
            final HlsMultivariantPlaylist playlist) {
        final List<HlsMultivariantPlaylist.Variant> variants = playlist.variants;
        if (variants.size() <= 1 || variants.stream().noneMatch(v -> hasVideo(v.format))) {
            return playlist;
        }

        int cheapestIndex = 0;
        for (int i = 1; i < variants.size(); i++) {
            if (isCheaper(variants.get(i).format, variants.get(cheapestIndex).format)) {
                cheapestIndex = i;
            }
        }

        // copy() keeps only the streams with a key, so keep the audio and subtitle renditions too
        final List<StreamKey> streamKeys = new ArrayList<>();
        streamKeys.add(new StreamKey(HlsMultivariantPlaylist.GROUP_INDEX_VARIANT, cheapestIndex));
        for (int i = 0; i < playlist.audios.size(); i++) {
            streamKeys.add(new StreamKey(HlsMultivariantPlaylist.GROUP_INDEX_AUDIO, i));
        }
        for (int i = 0; i < playlist.subtitles.size(); i++) {
            streamKeys.add(new StreamKey(HlsMultivariantPlaylist.GROUP_INDEX_SUBTITLE, i));
        }
        return playlist.copy(streamKeys);
    }

    private static boolean isCheaper(final Format format, final Format other) {
        if (hasHeAacAudio(format) != hasHeAacAudio(other)) {
            return !hasHeAacAudio(format);
        }
        return getBitrate(format) < getBitrate(other);
    }

    private static boolean hasVideo(final Format format) {
        return format.height > 0
                || Util.getCodecsOfType(format.codecs, C.TRACK_TYPE_VIDEO) != null;
    }

    private static boolean hasHeAacAudio(final Format format) {
        for (final String codec : Util.splitCodecs(
                Util.getCodecsOfType(format.codecs, C.TRACK_TYPE_AUDIO))) {
            // HE-AAC v1 and v2
            if (codec.equals("mp4a.40.5") || codec.equals("mp4a.40.29")) {
                return true;
            }
        }
        return false;
    }

    private static int getBitrate(final Format format) {
        return format.bitrate == Format.NO_VALUE ? Integer.MAX_VALUE : format.bitrate;
    }
}
