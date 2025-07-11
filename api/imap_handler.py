from imap_tools import MailBox, AND
import httpx

address = "crm@apexdiabetes.ru"
password = "1Mcl552smPjUsPXu"
server = "mail.netangels.ru"

api = "https://b24-d1uwq7.bitrix24.ru/rest/1/lh1jmrsp8p01x3j3/"
# Get date, subject and body len of all emails from INBOX folder
async def imap_handler():
    with MailBox(server).login(address, password) as mailbox:
        for msg in mailbox.fetch():
            print(msg.date, msg.subject, len(msg.text or msg.html))
            print(msg.html)

async def create_deal(data):
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{api}crm.deal.add", json=data)

async def get_data(text):
    print("#")
