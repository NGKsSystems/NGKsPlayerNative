import re 
with open('src/ui/main.cpp','r',encoding='utf-8') as f: src=f.read() 
src=src.replace('#include \x3cQApplication\x3E','#include \x3cQApplication\x3E\n#include \x22AnalysisBridge.h\x22') 
open('src/ui/main.cpp','w',encoding='utf-8').write(src) 
