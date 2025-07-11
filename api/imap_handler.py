from imap_tools import MailBox, AND

address = "crm@apexdiabetes.ru"
password = "1Mcl552smPjUsPXu"
server = "mail.netangels.ru"
# Get date, subject and body len of all emails from INBOX folder
async def imap_handler():
    with MailBox(server).login(address, password) as mailbox:
        for msg in mailbox.fetch():
            print(msg.date, msg.subject, len(msg.text or msg.html))
