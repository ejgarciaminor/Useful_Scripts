import tkinter as tk
from tkinter import messagebox
import datetime

def semana_actual():

    fecha_actual = datetime.datetime.now()
    n_semana = fecha_actual.strftime("%W")
    # n_semana = fecha_actual.isocalendar()[1] --> Con esto damos la semana ISO
    weekday = fecha_actual.isocalendar()[2]
    mensaje = []
    if weekday == 1:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Lunes"))
        #print(mensaje)
    elif weekday ==2:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Martes"))
        #print(mensaje)
    elif weekday ==3:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Miércoles"))
        #print(mensaje)
    elif weekday ==4:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Jueves"))
        #print(mensaje)
    elif weekday ==5:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Viernes"))
        #print(mensaje)
    elif weekday ==6:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Sábado"))
        #print(mensaje)
    else:
        mensaje.append((f"Estamos en la semana número:  {n_semana} y a día: Domingo"))
        #print(mensaje)
    messagebox.showinfo(message=mensaje)
   

root = tk.Tk()
root.title("Número de semana del año y día de la semana")

boton_mostrar =tk.Button(root, text ="Mostrar", command=semana_actual).grid(column =0,row=0)


boton_quit = tk.Button(root,text="Salir",command=root.destroy).grid(column=1,row=0)


root.mainloop()
