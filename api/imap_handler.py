from imap_tools import MailBox, AND
import httpx
import re

address = "crm@apexdiabetes.ru"
password = "1Mcl552smPjUsPXu"
server = "mail.netangels.ru"
#email - UF_CRM_1723114805999
#name - UF_CRM_1723114789182
#phone - UF_CRM_1723114796732
api = "https://b24-d1uwq7.bitrix24.ru/rest/1/lh1jmrsp8p01x3j3/"
# Get date, subject and body len of all emails from INBOX folder
async def imap_handler():
    with MailBox(server).login(address, password) as mailbox:
        for f in mailbox.folder.list():
            print(f)
        uids = []
        for msg in mailbox.fetch():
            print(msg.date, msg.subject, len(msg.text or msg.html))
            #print(msg.html)
            html = str(msg.html)
            print(len(html))
            print("Name" in html)
            name = re.findall("Name:(.*)<br>", html)[0]    
            phone = re.findall("Phone:(.*)<br>", html)[0]
            email = re.findall("Email:(.*)<br>", html)[0]
            comments = re.findall("Input:(.*)<br>", html)[0]
            
            input_2 = re.findall("Input_2:(.*)<br>", html)
            if len(input_2) > 0:
                comments += ("\n" + input_2[0])
            #+ "\n" + re.findall("Input_2:(.*)<br>", html)
            print(name, phone, email, comments)
            mailbox.move(msg.uid, "INBOX.Trash")
            await create_deal(name)

async def create_deal(name, phone, email, comments):
    async with httpx.AsyncClient() as client:
        data = {"fields": {"TITLE": name, "CATEGORY_ID": 12, "UF_CRM_1723114789182": name, "UF_CRM_1723114805999": email, "UF_CRM_1723114796732": phone, "COMMENTS": comments   }}
        response = await client.post(f"{api}crm.deal.add", json=data)

async def get_data(text):
    print("#")
