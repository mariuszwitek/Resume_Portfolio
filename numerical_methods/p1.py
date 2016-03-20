import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import numpy.linalg as npla
import scipy.linalg as spla
import numpy.polynomial.legendre as le

def f(x,y):
	return np.exp(-x*y)

def fs(r,theta):
	return r*np.exp(-r**2*np.cos(theta)*np.sin(theta))

def T(k,j,M):
	M[k][j] = (4.**j*M[k][j-1]-M[k-1][j-1])/(4**j-1)

def trap(i,g,a,b, yval):
	#if (i == 0):
	#	return (b-a)/2*(g(a)+g(b))
	xs = np.linspace(a,b,i)
	ys = np.ones(i)*yval
	y = g(xs,ys)
	return np.trapz(y,x=xs)

def fillM(s,g, a, b, yval):
	M = np.zeros((s,s))
	for i in range(s):
		M[i][0] = trap(i,g,a,b,yval)
		for j in range(1,i+1):
			T(i,j,M)
	return M

#Guass square
l = 50
b = 1.
a = 0
x, w = le.leggauss(l)
xs = (b-a)/2*x + (a+b)/2
s = 0
for i in range(len(xs)):
	for j in range(len(xs)):
		s = s + w[i]*w[j]*f(xs[i],xs[j])
I_sg = (b-a)*(b-a)*s/(2*2)
print "Gauss (a) = " + str(I_sg)

#Guass circle
l = 50
b = 1.
a = 0
x, w = le.leggauss(l)
ys = (b-a)/2*x + (a+b)/2
s = 0
for i in range(l):
	c = 0
	d = (1-ys[i]**2)**.5
	xs = (d-c)/2*x + (c+d)/2
	for j in range(l):
		s = s + (d-c)/2*w[i]*w[j]*f(xs[j],ys[i])
I_cg = (b-a)*s/2

print "Gauss (b) = " + str(I_cg)

#Romberg square
l = 50
ys = np.linspace(0,1,l)
ysf = np.zeros(l) 
for i in range(l):
	Tz = fillM(l,f,0,1,ys[i])
	ysf[i] = Tz[-1][-1]
I_sr = np.trapz(ysf,x=ys)

print "Romberg (a) = " + str(I_sr)

#Romberg circle
l = 50
ys = np.linspace(0,1,l)
ysf = np.zeros(l) 
for i in range(l):
	Tz = fillM(l,f,0,(1-ys[i]**2)**.5,ys[i])
	ysf[i] = Tz[-1][-1]
I_cr = np.trapz(ysf,x=ys)

print "Romberg (b) = " + str(I_cr)

