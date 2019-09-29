import re
import tkinter as tk
from tkinter import StringVar
import logging
import os
import configparser

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        config=configparser.ConfigParser()
        config.read(filenames='appconfig.ini')

        self.proxies = config.get("web_proxies","proxies").split(',')
        self.envs = config.get("env_list","env_type").split(',')

        self.lbl_proxy = tk.Label(self,text="Select Proxy:")
        #default value for proxy
        var_lst_proxy = StringVar(self)
        var_lst_proxy.set(self.proxies[0])
        self.lst_proxy = tk.OptionMenu(self,var_lst_proxy,*self.proxies)

        self.lbl_env = tk.Label(self,text="Select Environment:")
        #default value for env
        var_lst_env = StringVar(self)
        var_lst_env.set(self.envs[0])
        self.lst_env = tk.OptionMenu(self,var_lst_env,*self.envs)

        self.lbl_email = tk.Label(self,text="Enter Email:")
        self.email_pattern = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        vcmd = (self.register(self.email_validator),"%P","%i")
        self.txt_email = tk.Entry(self)#,validate="key",validatecommand=vcmd)

        self.lbl_passwd = tk.Label(self,text="Enter Password:")
        self.entry_passwd = tk.Entry(self,show="*")        

        self.btn_login = tk.Button(self,text="LogIn",command=self.do_login)

        self.lbl_proxy.grid(row=0,column=0)
        self.lst_proxy.grid(row=0,column=1)
        self.lbl_env.grid(row=1,column=0)
        self.lst_env.grid(row=1,column=1)

        self.lbl_email.grid(row=2,column=0)
        self.txt_email.grid(row=2,column=1)
        self.lbl_passwd.grid(row=3,column=0)
        self.entry_passwd.grid(row=3,column=1)
        self.btn_login.grid(row=5,column=0,columnspan=2)


        

    def email_validator(self,emailaddr,index):
        print("char {} at index {}".format(emailaddr,index))
        return self.email_pattern.match(emailaddr) is not None

    def do_login(self):
        pass

if __name__=="__main__":
    app=App()
    col_count, row_count = app.grid_size()

    for col in range(col_count):
        app.grid_columnconfigure(col, minsize=20)

    for row in range(row_count):
        app.grid_rowconfigure(row, minsize=20)

    app.update()
    app.minsize(app.winfo_width(),app.winfo_height())
    app.mainloop()