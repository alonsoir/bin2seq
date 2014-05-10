#!/usr/bin/python
import sys
import cv
import StringIO
import Image
import hashlib

def main():
    hash=hashlib.sha1()
    buff=StringIO.StringIO()
    buff.write(sys.stdin.read()) #STDIN to buffer
    hash.update(buff.getvalue())
    buff.seek(0)
    pil_im=Image.open(buff)
    cv_im = cv.CreateImageHeader(pil_im.size, cv.IPL_DEPTH_8U, 3)
    cv.SetData(cv_im, pil_im.tostring())
    cascade = cv.Load("../src/main/resources/haarcascade_frontalface_default.xml")
    print hash.hexdigest()+":"+str(cv.HaarDetectObjects(cv_im, cascade, cv.CreateMemStorage(0), 1.2, 2, 0, (50, 50)))

if __name__=="__main__":
    main()
