from math import sqrt
import pickle, copyreg, ssl

class zipCodeTree:
    """
        Creates a binary search tree to find zipcodes intersecting an area. 
    """
    maxDepth = 8 # Maximal depth of the search tree
    
    def __init__(self,zipcodelist,depth =0):
        """
            Takes a zipcodelist and the depth of the node
        """
        #Stores the extremal values of latitude and longitude of the zip codes.
        self.minLat = min(zipcodelist,key = lambda x: x.minLatitude).minLatitude
        self.minLon = min(zipcodelist,key = lambda x: x.minLongitude).minLongitude
        self.maxLat = max(zipcodelist,key = lambda x: x.maxLatitude).maxLatitude
        self.maxLon = max(zipcodelist,key = lambda x: x.maxLongitude).maxLongitude
        
        #print('Tree box depth ',depth,':',self.minLat, self.minLon, self.maxLat, self.maxLon)
        
        #If the number of zipcodes is small or the maxDepth is reach. returns a leaf node.
        length = len(zipcodelist) 
        if length<100 or depth>=zipCodeTree.maxDepth:
            self.zclist = zipcodelist
            self.terminal = True
            return
        self.terminal = False

        #Decides to splits the data along latitude or longitude depending on the largest width.
        if self.maxLon-self.minLon < self.maxLat-self.minLat:
            sortedlist = sorted(zipcodelist,key=lambda x: x.latitude)
            self.type = 'latitude'        
        else:
            sortedlist = sorted(zipcodelist,key=lambda x: x.longitude)
            self.type = 'longitude'

        #Creates the two children trees.
        self.left = zipCodeTree(sortedlist[:length//2],depth+1)
        self.right = zipCodeTree(sortedlist[length//2:],depth+1)
        
    def intersects(self,minmaxlatlong):
        """
            Returns the list of zipcodes intersecting the 'minmaxlatlong' area
        """
        minLat, minLon, maxLat, maxLon = minmaxlatlong
        if self.maxLat<minLat or self.minLat > maxLat or self.maxLon<minLon or self.minLon > maxLon:
             return []
        if self.terminal:
             return list(filter(lambda x:x.intersects(minmaxlatlong),self.zclist))
        else:
             return self.left.intersects(minmaxlatlong)+ self.right.intersects(minmaxlatlong)

        
    

class zipCode:
    '''
        Class to deal with zip codes.
    '''
    def __init__(self,csvrow):
        """
            Create a object from the csv row.
        """
        self.raw = csvrow
        data = csvrow.split(',')
        self.number = data[0]
        self.area = int(data[1])
        self.population = int(data[5])
        self.latitude = float(data[7])
        self.longitude = float(data[8])
    
    def __str__(self):
        return self.number

    @property
    def sideLength(self):
        '''
        return the length of a side of the zipcode area, considered to be a square.
        '''
        return sqrt(self.area)

    @sideLength.setter
    def sideLength(self,sideLength):
        self.area = sideLength**2
    
    @staticmethod
    def meterToDegreeConvertor(length):
        '''
            Convert meters in degrees
        '''
        meterToDegreeRatio = 110000
        return length/meterToDegreeRatio
    
    @property
    def sideLengthInDegrees(self):
        return zipCode.meterToDegreeConvertor(self.sideLength) 

    @property
    def minLatitude(self):
        return self.latitude - self.sideLengthInDegrees

    @property
    def maxLatitude(self):
        return self.latitude + self.sideLengthInDegrees

    @property
    def minLongitude(self):
        return self.longitude - self.sideLengthInDegrees

    @property
    def maxLongitude(self):
        return self.longitude + self.sideLengthInDegrees

    @staticmethod
    def _intersectionmatch(min_coord_im,max_coord_im,min_coord_zip,max_coord_zip):
        """
            Return True if two ranges overlap, False otherwise
        """
        return min_coord_im < max_coord_zip and max_coord_im > min_coord_zip

    def intersects(self,minmaxlatlong):
        """
            Return a boolean. True if the zipcode Area intersects the box, False otherwise.
        """
        min_lat, min_long, max_lat, max_long = minmaxlatlong
        latitudematch =  zipCode._intersectionmatch(min_lat,max_lat,self.minLatitude,self.maxLatitude)
        longitudematch =  zipCode._intersectionmatch(min_long,max_long,self.minLongitude,self.maxLongitude)
        return latitudematch and longitudematch

if __name__=='__main__':
    zipcoderow='00601,166659789,799296,64.35,0.31,18570,7744,18.180556,-66.749961'
    a = zipCode('00601,166659789,799296,64.35,0.31,18570,7744,18.180556,-66.749961')
    print(a.number,a.latitude,a.longitude,a.area)
        
        

