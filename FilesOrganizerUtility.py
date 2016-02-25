'''
   Copyright (C) 2016 Amit Thakur (https://github.com/amitthk)
      This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.
      This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA
'''

from PySide.QtCore import *
from PySide.QtGui import *
import sys, os, functools
from shutil import copy, copyfile
from PySide import QtCore


__appname__ = "File Organizer Utility"

class Thread(QThread):
    
    srcRoot='C:\\'
    destRoot = 'C:\\'
    fileExtList =['rtf','txt','doc','pdf','pod', 'mp3','mp4','flv']
    
    def run(self,srcRoot,destRoot, label):
        self.srcRoot=srcRoot
        self.destRoot=destRoot
        self.statusLabel=label
        self.updateLabel(self.srcRoot,self.statusLabel)
        for dirName, subdirList, fileList in os.walk(self.srcRoot):
            self.updateLabel('Processing: %s' % dirName,self.statusLabel)
            for thisFileName in fileList:
                thisFileExtn= os.path.splitext(thisFileName)[1][1:]
                self.updateLabel('FileName: {0}, Extension: {1}'.format(thisFileName,thisFileExtn),self.statusLabel)
                if(thisFileExtn in self.fileExtList):
                    srcfile=os.path.join(dirName,thisFileName)
                    destsubdir=os.path.join(self.destRoot,thisFileExtn)
                    if not(os.path.exists(destsubdir)):
                        os.makedirs(destsubdir)
                    destfilename=os.path.join(destsubdir,thisFileName)
                    if not(os.path.exists(destfilename)):
                        self.updateLabel('Copying file {0} to {1}'.format(srcfile,destsubdir),self.statusLabel)
                        copy(srcfile,destsubdir)
                    else:
                        ii=1
                        while True:
                            new_name = os.path.join(os.path.splitext(destfilename)[0] + "_" + str(ii) + '.'+thisFileExtn)
                            if not(os.path.exists(new_name)):
                                self.updateLabel('Copying file {0} to {1}'.format(srcfile,new_name),self.statusLabel)
                                copy(srcfile, new_name)
                                break 
                            ii += 1
                            
        self.updateLabel('And we\'re done processing! Yeah!',self.statusLabel)
        
    def updateLabel(self, txt, label):
        label.append(txt)
        label.update()
        QtCore.QCoreApplication.processEvents()

class form(QDialog):
        
    srcRoot='C:\\'
    destRoot = 'C:\\'
    fileExtList =['mp3','mp4']
    def __init__(self,parent=None):
        super(form,self).__init__(parent)
        
        srcButton = QPushButton("Source")
        self.lblSource= QLabel(self.srcRoot)
        destButton = QPushButton("Destination")
        self.lblDest= QLabel(self.destRoot)
        goButton = QPushButton("Run!")
        
        self.lblStatus= QTextEdit()
        self.lblStatus.setReadOnly(True)
        self.lblStatus.setLineWrapMode(QTextEdit.NoWrap)

        closeButton = QPushButton("Close")
        
        srcButton.clicked.connect(self.get_source)
        destButton.clicked.connect(self.get_dest)
        goButton.clicked.connect(self.traverse)
        closeButton.clicked.connect(app.exit)
        
        layout = QVBoxLayout()
        layout.addWidget(srcButton)
        layout.addWidget(self.lblSource)
        layout.addWidget(destButton)
        layout.addWidget(self.lblDest)
        layout.addWidget(goButton)
        layout.addWidget(self.lblStatus)
        layout.addWidget(closeButton)
        self.setLayout(layout)
    
    def traverse(self):
        Thread().run(self.srcRoot, self.destRoot, self.lblStatus)
        
    def get_source(self):
        self.srcRoot=str(QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.lblSource.setText(self.srcRoot)
        
    def get_dest(self):
        self.destRoot=str(QFileDialog.getExistingDirectory(self, "Select Directory"))
        self.lblDest.setText(self.destRoot)
    
app = QApplication(sys.argv)
form=form()
form.show()
app.exec_()