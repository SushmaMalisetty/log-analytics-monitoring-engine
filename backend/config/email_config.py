from typing import Dict
from email.message import EmailMessage
import smtplib

SMTP_SERVER = "sushmamalisetty1705@gmail.com"
SMTP_PORT = 465
EMAIL = "malisettysushma@gmail.com"
PASSWORD = "xcez kyuh oxez hpyb"

def send_anomaly_email(to_email: str, anomaly: Dict):
    subject = "🚨 Log Anomaly Detected"

    body = f"""
🚨 Anomaly Detected in System Logs

Time Window : {anomaly['timestamp']}
Error Count : {anomaly['error_count']}
Z-Score     : {round(anomaly['z_score'], 2)}

Please investigate immediately.

Regards,
Log Monitoring System
"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(EMAIL, PASSWORD)
        server.send_message(msg)

#what is port number: port number that tells to the computer which server  or application should recive  the data .
#example: buliding address  -->IP address {system} 
#flat number --> port number {application}
#delivery boy --> data packets 
# IN computer terms ---> one computer run many services or applications at the same time , either websites or emails or any other file transfers
# All use same IP address but different port numbers .
# Some Port Numbers Used For : 
#1.80 for HTTP (web traffic)
#2.443 for HTTPS (secure web traffic)
#3.25 for SMTP (email sending) { old pport number for email sending }
#4.587 for SMTP (email sending with encryption) { new port number for email sending },  25, 587 465  these three are used for email sending and 587 , 467 these two are new port numbers for email sending , 25 is not secure 
#5.110 for POP3 (email receiving) 25 is only for sending email
#6.143 for IMAP (email receiving)
#why we use port number: 
#1)process to process communication
#2)port number is a unique identifier for a  network
#3)enable communication between devices
#4)it specifies server address 
#  5)port number are also used for avoid confusion between different services running on the same machine.
#
#status code:  status code is a three digit number that is sent by the server to the client in response to a request made by the client. It indicates the status of the request and whether it was successful or not.
# status codes are numbers that tells as what happend when a request meet to the server .
#how it works: when you open a website your browser send a request to the server then server responds withh the status code.
#200 OK :successful request {200 series 200 to 299}.
#why 100 , 300, 400 series {Task}.
#smtplib : it is library which is used to connect gmail with smtp server.
#starttls() : it is used to start a secure connection with the server.{server.starttls(sender , password) }.
 