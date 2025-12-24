from pycomm3 import LogixDriver
import time



plc = LogixDriver('141.141.141.128')

plc.open()
#plc.write("HMI_Suction_Start_Deg",210.0)
print(plc.read("HMI_Filling_Start_Deg"))

#plc.write("HMI_I_Start",True)
#time.sleep(2)
#plc.read("HMI_I_Start")
#plc.write("HMI_I_Start",False)
plc.close()
#plc.read("HMI_I_Start")
