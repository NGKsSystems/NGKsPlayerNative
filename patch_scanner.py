import sys

file_path = r'src\ui\library\LibraryScanner.cpp'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

FALLBACK = """
    // Fallback if no TLEN tag was found
    if (track.durationMs <= 0 && track.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
        QFile fFallback(track.filePath);
        if (fFallback.open(QIODevice::ReadOnly)) {
            const qint64 fileSize = fFallback.size();
            qint64 dataOff = pos; // pos is already advanced past ID3 headers in readId3Tags
            fFallback.seek(dataOff);
            QByteArray buf = fFallback.read(8192);
            int bitrate = 0;
            for (int i = 0; i < buf.size() - 3; ++i) {
                if ((static_cast<unsigned char>(buf[i]) == 0xFF) && ((static_cast<unsigned char>(buf[i+1]) & 0xE0) == 0xE0)) {
                    int bitrateIdx = (static_cast<unsigned char>(buf[i+2]) >> 4) & 0x0F;
                    int layer = (static_cast<unsigned char>(buf[i+1]) >> 1) & 0x03;
                    int ver = (static_cast<unsigned char>(buf[i+1]) >> 3) & 0x03;
                    static const int bitrates[2][3][16] = {
                        { {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,32,48,56,64,80,96,112,128,144,160,176,192,224,256,0} },
                        { {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0}, {0,32,48,56,64,80,96,112,128,160,192,224,256,320,384,0}, {0,32,64,96,128,160,192,224,256,288,320,352,384,416,448,0} }
                    };
                    int mpegIdx = (ver == 3) ? 1 : 0;
                    int layerIdx = (layer == 1) ? 0 : ((layer == 2) ? 1 : 2);
                    bitrate = bitrates[mpegIdx][layerIdx][bitrateIdx];
                    break;
                }
            }
            if (bitrate <= 0) bitrate = 256;
            track.durationMs = static_cast<qint64>(((fileSize - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
            int totalSec = static_cast<int>(track.durationMs / 1000);
            track.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
        }
    }
"""

if 'Fallback if no TLEN tag' not in content:
    target = 'if (!track.artist.isEmpty() && !track.title.isEmpty()) {'
    parts = content.split(target)
    if len(parts) == 2:
        new_content = parts[0] + FALLBACK + "\n    " + target + parts[1]
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Injected MP3 length fallback patch.")
    else:
        print("Target point not found.")
else:
    print("Already patched.")

"""
