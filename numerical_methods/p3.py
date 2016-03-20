import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import numpy.linalg as npla
import scipy.linalg as spla

t = np.linspace(0,np.pi/2,5,endpoint=True)

A = np.vander(t,5)
y = np.sin(t)

x = npla.solve(A,y)

def f(c,v):
	return v**4*c[0]+v**3*c[1]+v**2*c[2]+v**1*c[3]+c[4]
print "Ax=y"
print "A="
print A
print "y="
print y
print "x"
print x
e = np.random.rand(10)
print "error points"
print e
errs = np.abs(f(x,e)-np.sin(e))
print "errors"
print errs
print "max error"
print np.max(errs)
