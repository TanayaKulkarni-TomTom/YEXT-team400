import paramiko
paramiko.util.log_to_file("paramiko.log")

# Open a transport
host,port = " ",22 #enter host
transport = paramiko.Transport((host,port))

# Auth    
username,password = "***","*******" #Enter credentials
transport.connect(None,username,password)

# Download
filepath = " " #Path for sftp folder where file resides
localpath = r" " #Path for destination folder

#Create function without parameters which returns the local path of the downloaded file if it is downloaded
def sftp_filepath():
 # Go!    
 sftp = paramiko.SFTPClient.from_transport(transport)
 files=sftp.listdir()
 if files:
    file=files[0]
 try:
    sftp.get(filepath,localpath)
    return localpath
    # Close
    if sftp: sftp.close()
    if transport: transport.close()
 except:
   raise
 


