from abc import ABC, abstractmethod
import math

class AreaAndVolume(ABC):
    @abstractmethod
    def area(self):
        pass
    @abstractmethod
    def volume(self):
        pass

class cylinder(AreaAndVolume):
    def __init__(self, radius, height):
        self.radius = radius
        self.height = height
    
    def area(self):
        return math.ceil((2*(22/7)*self.radius*self.height) + 2*((22/7)*self.radius**2))

    def volume(self):
        return math.ceil((22/7)*self.radius**2*self.height)
    
class sphere(AreaAndVolume):
    def __init__(self, radius):
        self.radius = radius
    
    def area(self):
        return 4*(22/7)*self.radius**2

    def volume(self):
        return (4/3)*(22/7)*self.radius**3
    

sphere1 = sphere(5)
cylinder1 = cylinder(5, 10)
print("Sphere Area:", sphere1.area())
print("Sphere Volume:", sphere1.volume())
print("Cylinder Area:", cylinder1.area())
print("Cylinder Volume:", cylinder1.volume())