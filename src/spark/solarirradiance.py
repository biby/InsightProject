from math import cos, sin,pi, acos
import datetime

def degToRad(angle):
    return angle*pi/180

def radToDeg(angle):
    return angle*180/pi

def nbDaysSinceBeginingOfYear(date):
    year = dt.year
    NewYearsEve = datetime.date(year-1,12,31)
    return date-NewYearsEve
    
def earthTilt(date):
    nbDaysInYear= nbDaysSinceBeginingOfYear(date)
    return 23.45*sin(2*pi*(284+nbDaysInYear)/365)

def theoreticalIrradiance(date,latitude,moduleAngle):
    '''
        Returns the theoretical maximal iradiance (solar noon) at sea level, depending on the latitude, date and installation angle of the module. 
        Used foormulae can be found on the website: www.pveducation.org.
    '''
    S = 1000 #Solar Constant at sea level (in W/m2)
    nbDaysInYear= nbDaysSinceBeginingOfYear(date)
    yearlyVariationFactor = .034*cos(2*pi*nbDaysInYear/365)
    earthTilt = earthTilt(date)
    angleFactor = max(cos(degToRad(latitude-earthTilt-moduleAngle)),0)
    return S*angleFactor*(1+yearlyVariationFactor)
   

def dayTime(date, latitude):
    '''
        returns the length of the day in hours as a float
    '''
    nbDaysInYear= nbDaysSinceBeginingOfYear(date)
    earthTilt = earthTilt(date)
    x = - (sin(degToRad(latitude)) * sin(degToRad(eathTilt)))
    x = x / (cos(degToRad(latitude)) * cos(degToRad(eathTilt)))
    #Taking care of full day of sun or night problems	
    if x > 1.0:
        x = 1.0
    if x < -1.0:
        x = -1.0
    f = acos(x)
    H = radToDeg(f/15.0)
    return 2*H


def sunrise(date,latitude):
    return 12-dayTime(date,latitude)/2


def sunset(date,latitude):
    return 12+dayTime(date,latitude)/2

def  currentRadiation( date,  latitude,  hour, moduleAngle):
    sr = sunrise(day, lat)
    ss = sunset(day, lat)
    if hour < sr:
        return 0.0
    if hour > ss:
        return 0.0
    hourfactor = sin((hour-sr)*pi/ss) #Use a sinusoid for the the day variation. May be improved.
    """am = AM(hour, day, lat)
    x1 = 0.7**(am*0.678)""" 
    return theoreticalIrradiance(date,latitude,moduleAngle)*hourfactor

def radiation(date, latitude, hour, moduleAngle, cloudCoverage):
    cloudFreeRadiation = currentRadiation( date,  latitude,  hour, moduleAngle)
    cloudFactor = 1-.75*cloudCoverage**3
    return cloudFreeRadiation*cloudFactor


