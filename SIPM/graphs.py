import numpy as np
import matplotlib.pyplot as plt
import seaborn

pptoq_raw = np.genfromtxt('PPtoQ.csv', delimiter=',')
ppvsb = np.genfromtxt('GainVsBias_csv.csv', delimiter=',')
areas = np.genfromtxt('dark_areas.csv', delimiter=',')

ppvsb = ppvsb.T
areas = areas.T
bv = areas[0]

pptoq_raw = pptoq_raw.T
pptocv = np.zeros((2, pptoq_raw.shape[1]))

pptocv[0,:] = pptoq_raw[1,:] #1->p1 2->p2 2-1->diff
pptocv[1,:] = 10*10**-12*pptoq_raw[0,:]*10**-3/(1.602*10**-19)

pptoq_m, pptoq_b = np.polyfit(pptocv[0], pptocv[1], 1)

temp = np.array([-159, -116, -96, -163, -140, -120, -102])
temp = temp + 273
temp_size = temp.shape[0]
temp_a = np.array([-163, -140, -120, -102])
temp_a = temp_a + 273
temp_a_size = temp_a.shape[0]

gvsb = np.zeros((1 + temp_size, ppvsb.shape[1]))
gvsb_lin = np.zeros((temp_size, 2))

bdvvst = np.zeros((temp_size))

gvsb[0,:] = ppvsb[0,:]
for i in range(temp_size):
	gvsb[i+1, :] = (pptoq_m*(ppvsb[2*i+2,:] - ppvsb[2*i+1,:]))*10**-6 #2*i+2->p1, 2*i+3->p2
	gn = np.logical_not(np.isnan(gvsb[i+1]))	
	gvsb_lin[i] = np.polyfit(gvsb[0,gn], gvsb[i+1,gn], 1)
	bdvvst[i] = -gvsb_lin[i,1]/gvsb_lin[i,0]

bdvvst_lin = np.polyfit(temp, bdvvst, 1)
print bdvvst_lin
bdvs = bdvvst_lin[0]*temp+bdvvst_lin[1]

gvsbd2 = (bdvs + 2)*gvsb_lin[:,0]+gvsb_lin[:,1]
gvsbd3 = (bdvs + 3)*gvsb_lin[:,0]+gvsb_lin[:,1] 
gvsbd4 = (bdvs + 4)*gvsb_lin[:,0]+gvsb_lin[:,1] 

dc05BT = np.zeros((temp_a_size,areas.shape[1]))
dc15BT = np.zeros((temp_a_size,areas.shape[1]))
dc05BT_fit = np.zeros((temp_a_size,2))
dc15BT_fit = np.zeros((temp_a_size,2))
cor = np.zeros((temp_a_size ,bv.shape[0]))

for i in range(temp_a_size):
	gn = np.logical_not(np.isnan(areas[3*i+3]))	
	dc05BT[i,gn] = areas[3*i+1,gn]/(areas[3*i+3,gn]*36)
	dc15BT[i,gn] = areas[3*i+2,gn]/(areas[3*i+3,gn]*36)
	cor[i,gn] = 100*areas[3*i+2,gn]/areas[3*i+1,gn]
	dc05BT_fit[i] = np.polyfit(bv[gn], np.log(dc05BT[i,gn]), 1)	
	dc15BT_fit[i] = np.polyfit(bv[gn], np.log(dc15BT[i,gn]), 1)	

dc052 = np.exp(dc05BT_fit[:,0]*(bdvs[3:] + 2) + dc05BT_fit[:,1])
dc053 = np.exp(dc05BT_fit[:,0]*(bdvs[3:] + 3) + dc05BT_fit[:,1])
dc054 = np.exp(dc05BT_fit[:,0]*(bdvs[3:] + 4) + dc05BT_fit[:,1])

dc152 = np.exp(dc15BT_fit[:,0]*(bdvs[3:] + 2) + dc15BT_fit[:,1])
dc153 = np.exp(dc15BT_fit[:,0]*(bdvs[3:] + 3) + dc15BT_fit[:,1])
dc154 = np.exp(dc15BT_fit[:,0]*(bdvs[3:] + 4) + dc15BT_fit[:,1])

colors = ['b', 'g', 'r', 'm', 'c', 'y', 'k', 'w']

f1 = plt.figure()
ax1 = f1.add_subplot(111)

f2 = plt.figure()
ax2 = f2.add_subplot(111)

f3 = plt.figure()
ax3 = f3.add_subplot(111)

f4 = plt.figure()
ax4 = f4.add_subplot(111)

f5 = plt.figure()
ax5 = f5.add_subplot(111)

f6 = plt.figure()
ax6 = f6.add_subplot(111)

f7 = plt.figure()
ax7 = f7.add_subplot(111)

f8 = plt.figure()
ax8 = f8.add_subplot(111)

f9 = plt.figure()
ax9 = f9.add_subplot(111)

ax1.set_title("Gain vs Bias Voltage varying Temperature")
ax1.set_ylabel("Gain x 10^6")
ax1.set_xlabel("Bias Voltage (V)")
n = np.argsort(temp)
for j in range(temp_size):
	i = n[j]
	ax1.plot(gvsb[0], gvsb[i+1], 'o', color = colors[i], label="Temp = " + str(temp[i])+ "K")
	ax1.plot(gvsb[0], gvsb_lin[i,0]*gvsb[0] + gvsb_lin[i,1], '-', color = colors[i])
ax1.legend(loc="upper left")

ax2.set_title("Breakdown Voltage vs Temperature")
ax2.set_ylabel("Breakdown Voltage (V)")
ax2.set_xlabel("Temperature (K)")
ax2.set_xlim([100,180])
ax2.plot(temp, bdvvst, 'o', color = 'b')
ax2.plot(temp, bdvs, '-', color = 'g')
ax2.errorbar(temp, bdvvst, yerr= .005*bdvvst, fmt = None, color = 'b')
print 

ax3.set_title("Gain vs Temperature for Constant over-voltage")
ax3.set_ylabel("Gain x 10^6")
ax3.set_xlabel("Temperature (K)")
ax3.plot(temp, gvsbd2, 'o', label = "Over Voltage = 2 V", color = colors[0])
ax3.errorbar(temp, gvsbd2, yerr= .05*gvsbd2, fmt = None, color = colors[0])
ax3.plot(temp, gvsbd3, 'o', label = "Over Voltage = 3 V", color = colors[1])
ax3.errorbar(temp, gvsbd3, yerr= .05*gvsbd3, fmt = None, color = colors[1])
ax3.plot(temp, gvsbd4, 'o', label = "Over Voltage = 4 V", color = colors[2])
ax3.errorbar(temp, gvsbd4, yerr= .05*gvsbd4, fmt = None, color = colors[2])
ax3.set_ylim([2,10])
ax3.legend(loc="lower center")

ax4.set_title("Dark Count (>.5 PEP) vs Bias Voltage varying Temperature")
ax4.set_ylabel("Dark Count (Hz/mm^2)")
ax4.set_xlabel("Bias Voltage (V)")
for i in range(temp_a_size):
	gn = dc05BT[i] != 0	
	ax4.semilogy(bv[gn], dc05BT[i,gn], 'o', color = colors[i], label="Temp = " + str(temp_a[i])+ "K")
	ax4.semilogy(bv[gn], np.exp(bv[gn]*dc05BT_fit[i,0]+dc05BT_fit[i,1]), '-', color = colors[i])
ax4.legend(loc="center left")

ax5.set_title("Correlated Noise vs Over Voltage")
ax5.set_ylabel("Cross talk Probability (%)")
ax5.set_xlabel("Over Voltage (V)")
for i in range(temp_a_size):
	gn = cor[i] != 0	
	ov = bv - bdvs[3+i]
	ax5.plot(ov[gn], cor[i,gn], 'o', color = colors[i], label="Temp = " + str(temp_a[i])+ "K")
#ax5.set_ylim(0,10)
ax5.legend(loc="center left")

ax6.set_title("Dark Count (>1.5 PEP) vs Bias Voltage varying Temperature")
ax6.set_ylabel("Dark Count (Hz/mm^2)")
ax6.set_xlabel("Bias Voltage")
for i in range(temp_a_size):
	gn = dc15BT[i] != 0	
	ax6.semilogy(bv[gn], dc15BT[i,gn], 'o', color = colors[i], label="Temp = " + str(temp_a[i])+ "K")
	ax6.semilogy(bv[gn], np.exp(bv[gn]*dc15BT_fit[i,0]+dc15BT_fit[i,1]), '-', color = colors[i])
ax6.legend(loc="center left")

ax7.set_title("Dark Count (>0.5 PEP) vs Temperature")
ax7.set_ylabel("Dark Count (Hz/mm^2)")
ax7.set_xlabel("Temperature (K)")
ax7.semilogy(temp_a, dc052, 'o',  label="Over Voltage = 2 V")
ax7.semilogy(temp_a, dc053, 'o',  label="Over Voltage = 3 V")
ax7.semilogy(temp_a, dc054, 'o',  label="Over Voltage = 4 V")
ax7.legend(loc="center left")

ax8.set_title("Dark Count (>1.5 PEP) vs Temperature")
ax8.set_ylabel("Dark Count (Hz/mm^2)")
ax8.set_xlabel("Temperature (K)")
ax8.semilogy(temp_a, dc152, 'o',  label="Over Voltage = 2 V")
ax8.semilogy(temp_a, dc153, 'o',  label="Over Voltage = 3 V")
ax8.semilogy(temp_a, dc154, 'o',  label="Over Voltage = 4 V")
ax8.legend(loc="center left")

ax9.set_title("Dark Count vs Bias Voltage for T = " + str(temp_a[3]) + "K")
ax9.set_ylabel("Dark Count (Hz/mm^2)")
ax9.set_xlabel("Bias Voltage (V)")
i = 3
gn = dc15BT[i] != 0	
ax9.semilogy(bv[gn], dc15BT[i,gn], 'o', color = colors[0], label=">1.5 PEP")
ax9.semilogy(bv[gn], np.exp(bv[gn]*dc15BT_fit[i,0]+dc15BT_fit[i,1]), '-', color = colors[0])
gn = dc05BT[i] != 0	
ax9.semilogy(bv[gn], dc05BT[i,gn], 'o', color = colors[1], label=">0.5 PEP")
ax9.semilogy(bv[gn], np.exp(bv[gn]*dc05BT_fit[i,0]+dc05BT_fit[i,1]), '-', color = colors[1])
ax9.legend(loc="upper left")


f1.savefig("f1.png")
f2.savefig("f2.png")
f3.savefig("f3.png")
f4.savefig("f4.png")
f5.savefig("f5.png")
f6.savefig("f6.png")
f7.savefig("f7.png")
f8.savefig("f8.png")
f9.savefig("f9.png")
plt.show()
