import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import numpy.linalg as npla
import scipy.linalg as spla
import scipy.interpolate as spi

def f(x):
	return 1./(1+25*(x**2))
	
n11 = np.linspace(1,11,11,endpoint=True)
n21 = np.linspace(1,21,21,endpoint=True)
n110 = np.linspace(1,110,110,endpoint=True)
n210 = np.linspace(1,210,210,endpoint=True)
a11 = np.linspace(-1,1,11,endpoint=True)
a21 = np.linspace(-1,1,21,endpoint=True)
c11 = np.cos((2*n11-1)*np.pi/(2*11))
c21 = np.cos((2*n21-1)*np.pi/(2*21))

fa11 = spi.lagrange(a11,f(a11))
fa21 = spi.lagrange(a21,f(a21))
fb11 = spi.interp1d(a11,f(a11), kind='cubic')
fb21 = spi.interp1d(a21,f(a21), kind='cubic')
fc11 = spi.lagrange(c11,f(c11))
fc21 = spi.lagrange(c21,f(c21))

p11 = np.linspace(-1,1,110,endpoint=True)
p21 = np.linspace(-1,1,210,endpoint=True)
pc11 = np.cos((2*n110-1)*np.pi/(2*110))
pc21 = np.cos((2*n210-1)*np.pi/(2*210))

f1 = plt.figure()
ax1 = f1.add_subplot(111)
ax1.plot(p11,f(p11),label='f')
ax1.plot(p11,fa11(p11),label='a')
ax1.plot(pc11,fb11(pc11),label='b')
ax1.plot(p11,fc11(p11),label='c')
ax1.legend()

f2 = plt.figure()
ax2 = f2.add_subplot(111)
ax2.plot(p21,f(p21),label='f')
ax2.plot(p21,fa21(p21),label='a')
ax2.plot(pc21,fb21(pc21),label='b')
ax2.plot(p21,fc21(p21),label='c')
ax2.legend()

f3 = plt.figure()
ax3 = f3.add_subplot(111)
ax3.plot(p11,f(p11)-fa11(p11),label='f-a')
ax3.plot(pc11,f(pc11)-fb11(pc11),label='f-b')
ax3.plot(p11,f(p11)-fc11(p11),label='f-c')
ax3.legend()

f4 = plt.figure()
ax4 = f4.add_subplot(111)
ax4.plot(p21,f(p21)-fa21(p21),label='f-a')
ax4.plot(pc21,f(pc21)-fb21(pc21),label='f-b')
ax4.plot(p21,f(p21)-fc21(p21),label='f-c')
ax4.legend()

print max(abs(f(p11)-fa11(p11)))
print max(abs(f(pc11)-fb11(pc11)))
print max(abs(f(p11)-fc11(p11)))
print max(abs(f(p21)-fa21(p21)))
print max(abs(f(pc21)-fb21(pc21)))
print max(abs(f(p21)-fc21(p21)))

plt.show()
