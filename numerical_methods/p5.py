import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import numpy.linalg as npla
import scipy.linalg as spla

#forward
s = int(4/.1)
tf = np.linspace(0,4,s,endpoint=True)
yf = np.zeros(s)
yf[0] = 1
for i in range(1,s):
	yf[i] = yf[i-1] + .1*(-5)*yf[i-1]

#backward
yb = np.zeros(s)
yb[0] = 1
for i in range(1,s):
	yb[i] = yb[i-1]/1.5
	
re = np.exp(-5*tf)

plt.plot(tf,yf,'r',label="forward")
plt.plot(tf,yb,'b',label="backward")
plt.plot(tf,re,'g',label="e^-5t")
plt.legend()
plt.show()
print yf[-1]
print yb[-1]
