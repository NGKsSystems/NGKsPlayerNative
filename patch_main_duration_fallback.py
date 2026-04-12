import sys
import re

file_path = r'src\ui\main.cpp'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

FALLBACK = """
                  // Fallback: For any tracks without duration, estimate it from MP3 filesize
                  for (auto& t : allTracks_) {
                      if (t.durationMs <= 0 && t.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
                          QFile fFallback(t.filePath);
                          if (fFallback.open(QIODevice::ReadOnly)) {
                              QByteArray hdr = fFallback.read(10);
                              qint64 dataOff = 0;
                              if (hdr.size() == 10 && hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
                                  dataOff = 10 + ((quint32(static_cast<unsigned char>(hdr[6])) << 21)
                                               | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                                               | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                                               | quint32(static_cast<unsigned char>(hdr[9])));
                              }
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
                              t.durationMs = static_cast<qint64>(((fFallback.size() - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
                              int totalSec = static_cast<int>(t.durationMs / 1000);
                              t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                          }
                      }
                  }
"""

pattern = r'(djDb_\.bulkInsert\(allTracks_\);)'

if 'For any tracks without duration' not in content:
    new_content = re.sub(pattern, FALLBACK + r'\n\1', content)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Injected MP3 duration fallback!")
else:
    print("Already patched.")

