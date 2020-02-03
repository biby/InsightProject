import numpy as np
from math import cos,pi

class satelliteImage:
    maxpixelintensity = 2**16 #Default maximum pixel
    
    def __init__(self,image,minmaxlatlon):
        self.minLatitude, self.minLongitude, self.maxLatitude, self.maxLongitude = minmaxlatlon
        self.image = image
    
    @property
    def verticalRatio(self):
        """
            Returns the ratio of vertical degree per pixel
        """
        return    (self.maxLatitude-self.minLatitude)/self.image.shape[0]

    @property
    def horizontalRatio(self):
        """
            Returns the ratio of horizontal degree per pixel
        """
        return    (self.maxLongitude-self.minLongitude)/self.image.shape[1]
    
    def latitudeToPixel(self,latitude):
        """
            Convert latitude to pixel coordinate
        """
        return int((latitude - self.minLatitude)/self.verticalRatio)
    
    def longitudeToPixel(self,longitude):
        """
            Convert longitude to pixel coordinate
        """
        return int((longitude - self.minLongitude)/self.horizontalRatio)

    def coordinateToPixels(self,latitude,longitude):
        """
            Convert latitude,longitude to pixel coordinates.
        """
        x = self.latitudeToPixel(latitude)
        y = self.longitudeToPixel(longitude)
        return (x,y)

    def __getitem__(self,latitude,longitude):
        x,y = coordinateToPixels(latitude,longitude)
        return self.image[x,y]
    
    def areaInPixels(self):
        '''
            Returns the number of pixel in the actual image. Takes into account that the image is tilted.
        '''
        return np.count_nonzero(self.image)
    
    def areaInSqMt(self):
        latitudedegreetometer = 111320
        longitudedegreetometer = 111320*cos(self.maxLatitude*pi/180)
        areainpx = self.areaInPixels()
        areainsqmt = int(areainpx*self.verticalRatio*latitudedegreetometer*self.horizontalRatio*longitudedegreetometer)
        return areainsqmt

    def cloudCover(self):
        '''
            Return the cloud coverage on the image
        '''
        maxpixelintensity = 2**16 #Default maximum pixel
        x,y = self.image.shape
        area = self.areaInPixels()
        total =np.sum(self.image) 
        return total/(area*maxpixelintensity) 

    def crop(self,minmaxlatlong):
        """
            return an image cropped along the given 'minmaxlatlong' box.
            
            minmaxlatlong should be a list containing the minimum latitude, minimum longitude, maximum latitude and maximum longitude.
        """
        minlat, minlon, maxlat, maxlon = minmaxlatlong
        #Takes the intersection of the box and the image.        
        minx = self.latitudeToPixel(minlat)        
        if minx <0:
            minx=0
            minlat = self.minLatitude    

        miny = self.longitudeToPixel(minlon)
        if miny <0:
            miny=0
            minlon = self.minLongitude    

        maxx = self.latitudeToPixel(maxlat)
        if maxx >=self.image.shape[0]:
            maxx=self.image.shape[0]
            minlat = self.minLatitude    

        maxy = self.longitudeToPixel(maxlon)
        if maxy >=self.image.shape[1]:
            maxy=self.image.shape[1]
            maxlon = self.maxLongitude

        croppedImage = self.image[minx:maxx,miny:maxy]
        cr = satelliteImage(croppedImage,[minlat, minlon, maxlat, maxlon])
        return cr


