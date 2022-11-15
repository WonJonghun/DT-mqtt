import tkinter as tk
import time, random
import psutil
import os
import RPi.GPIO as g
import netifaces

def getInfo():
    cpu = psutil.cpu_percent(interval=None,percpu=False)
    aval = psutil.virtual_memory().available
    temp = psutil.sensors_temperatures()["cpu_thermal"][0]
    disk = psutil.disk_usage('/')
    net_if = ['eth0', 'wlan0']
    netinfo = []
    for interface in net_if:
        if( 2 in netifaces.ifaddresses(interface)):
            for link in netifaces.ifaddresses(interface)[netifaces.AF_INET]:
                netinfo.append(link['addr'])
        else:
            netinfo.append("no ip")                     
    return str(cpu), str(round(temp.current, 1)), str(round((aval/1024**3),1)), str(round((disk.free/1024**3) ,1)), netinfo

def label_update():
    cpu_val, temp_val, Mem_val, disk_val, net_val = getInfo()
    msg3.set("CPU(%) : " + cpu_val)
    msg4.set("Temp(C) : " + temp_val)
    msg5.set("Memory avail.(GB) : " + Mem_val)
    msg6.set("Disk free(GB) : " + disk_val)
    msg7.set("Net : " + net_val[0] +", "+ net_val[1])
    if g.input(12) == True:
        g.output(12, False)
    else:
        g.output(12, True)
    label3.after(1000, label_update)

def on_closing():
    g.output(12, False)
    g.cleanup()
    root.destroy()

g.setmode(g.BCM)
g.setwarnings(False)
g.setup(12, g.OUT)
g.output(12, True)
root = tk.Tk()
msg1 = tk.StringVar()
msg2 = tk.StringVar()
msg3 = tk.StringVar()
msg4 = tk.StringVar()
msg5 = tk.StringVar()
msg6 = tk.StringVar()
msg7 = tk.StringVar()
msg1.set("DCU Operation ")
msg2.set("---------------------------------------------------------------")
msg3.set("CPU : ")
msg4.set("Temp : ")
msg5.set("Memory : ")
msg6.set("Disk free : ")
msg7.set("Network : ")
root.configure(bg='#2F5597', cursor='none')
root.attributes('-fullscreen', True )
root.protocol("WM_DELETE_WINDOW", on_closing)
label1=tk.Label(root, textvariable=msg1, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label2=tk.Label(root, textvariable=msg2, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label3=tk.Label(root, textvariable=msg3, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label4=tk.Label(root, textvariable=msg4, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label5=tk.Label(root, textvariable=msg5, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label6=tk.Label(root, textvariable=msg6, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label7=tk.Label(root, textvariable=msg7, font=('Arial', 16, 'bold'), bg="#2F5597", fg="white")
label1.place(x=10,y=20)
label2.place(x=10,y=45)
label3.place(x=10,y=90)
label4.place(x=10,y=130)
label5.place(x=10,y=170)    
label6.place(x=10,y=210)
label7.place(x=10,y=250)
label_update()
root.mainloop()
