from concurrent.futures import ThreadPoolExecutor,as_completed
import time
import os
import boto3
import tempfile
import PIL
from PIL import Image
import numpy as np
import pyspark.sql.functions
from itertools import islice
from satelliteImage import satelliteImage
from zipcode import zipCode

class ImageProcessor:
    """
        Image Processor optimize processing time by threading several downloads at a time to optimize bandwith use.
    """
    def __init__(self,zipsearch,nbThread=20):
        """
            Create a ThreadPool of nbThread for further imag download
        """
        self.tpe = ThreadPoolExecutor(nbThread)
        self.futures = []
        self.zipsearch = zipsearch

    def getImage(self,s3Bucket,url):
        """
            Download image at url from s3Bucket
        """
        
        start_time = time.time()
        s3 = boto3.client('s3')
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name,'wb') as f:
            s3.download_fileobj(s3Bucket, url, f)
        img = PIL.Image.open(tmp.name)   
        img_arr = np.array(img)
        print('download time: ', time.time()-start_time)
        return img_arr

    
    def getUrl(self,line):
        """
            extract url and S3 bucket from image string
        """
        l = line.split(',')
        url = l[-1]
        path = url.split('/')
        s3Bucket = path[3]
        path = path[4:] #Extract path in the s3 bucket
        fileName = path[-2]+'_B9.TIF'
        path[-1]=fileName
        url = '/'.join(path)
        return (url,s3Bucket)
    
    def getMinmaxlatlong(self,line):
        """
            Extract latitude longitude bounding box from image string
        """
        l = line.split(',')
        minmaxlatlong = list(map(float,l[7:11]))
        return minmaxlatlong   
        
    def intersectingZipCode(self,minmaxlatlong):
        """
            return the list of zipcode intersecting a latitude longitude box.
        """ 
        return list(map(lambda x: x.raw, self.zipsearch.intersects(minmaxlatlong)))
    
    def setUrlList(self,data):
        """
            Return a tuple (url, S3bucket, image string, intersecting zipcodes) for each image string in data.
            where url is the download url of image
                S3bucket is the S3 bucket associate to url
                image string is the raw data
                intersecting zipcodes is a list of zipcodes intersecting the image

            If no zipcode intersect an image then the image is ignored.
        """
        urllist = []
        for line in data:
            minmaxlatlong = self.getMinmaxlatlong(line)
            zips = self.intersectingZipCode(minmaxlatlong)
            if len(zips)>0:
                url,bucket = self.getUrl(line)
                urllist.append((url,bucket,line,zips))
        return urllist

    def crop(self,image,line,zp):
        """
            Given an image, image metadata and a zipcode return a tuple containing:
            Id of image
            zipcode
            timestamp of when the image was taken
            cloud coverage of the zipcode
            size of the area of the intersection of the image and the zipcode
        """
        l = line.split(',')
        minmaxlatlong = self.getMinmaxlatlong(line)   
        img = satelliteImage(image,minmaxlatlong)
        zipc = zipCode(zp)    
        croppedImg = img.crop([zipc.minLatitude, zipc.minLongitude, zipc.maxLatitude, zipc.maxLongitude])
        cover = croppedImg.cloudCover()
        area = croppedImg.areaInSqMt()
        if croppedImg.areaInPixels() ==0:
            return None
        #ID Zipcode timestamp, cover, area
        entityId = l[1]
        timeStamp = l[2]
        theoreticalcover = l[3]
        zipcode,zipcodearea = zp.split(',')[:2]
        return (entityId,zipcode,timeStamp,cover.item(),area,zipcodearea)#,theoreticalcover)
    
    def processImage(self,image,line,zips):
        """
            Process one image for all zipcodes it intersects
        """
        return [x for x in map(lambda zp: self.crop(image,line,zp),zips) if x is not None]

    def startDownload(self,urllist):
        """
            Threads the multiple downloads in a dictionary containing metadata
        """        
        self.futureDownload = {self.tpe.submit(self.getImage,bucket,url):(line,zips) for url, bucket, line, zips in urllist}

    def processBatch(self,data):
        """
            processes a batch of images by threading the imagesdownload and then processing images as they arrive.
        """
        start_time = time.time()
        urllist = self.setUrlList(data)
        self.startDownload(urllist)          
        res = []
        for future in as_completed(self.futureDownload):
            line,zips = self.futureDownload[future]
            image = future.result()
            res+=self.processImage(image,line,zips)
        print('Batch Processing Time: ', time.time()-start_time)
        return res

