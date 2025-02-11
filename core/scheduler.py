from calendar import SUNDAY
from datetime import datetime

from bs4 import BeautifulSoup
import urllib3
from django.db import connection


def your_job_function():
    # put your scheduled task code here
    print(
        "Scheduler is running! <your_job_function> ||"
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


def scrap_data_from_metal_market():
    """
    Scraps data from Metal Market
    """
    http = urllib3.PoolManager(cert_reqs="CERT_NONE")

    url = "https://www.metalsmarket.net/w_lmeCashSett.html"
    response = http.request("GET", url)
    soup = BeautifulSoup(response.data, "html.parser")

    # Find Currency And Date
    currency_td = soup.find("td", text="USD")
    date_td = currency_td.find_next_sibling()

    currency_value = currency_td.text.strip()
    date_value = date_td.text.strip()

    # Find ALUMINIUM bid and ask values
    alum_bid = soup.find("td", text="ALUM").find_next_sibling().text
    alum_ask = soup.find("td", text="ALUM").find_next_sibling().find_next_sibling().text

    # Find LEAD bid and ask values
    lead_bid = soup.find("td", text="LEAD").find_next_sibling().text
    lead_ask = soup.find("td", text="LEAD").find_next_sibling().find_next_sibling().text

    # Find COBALT bid and ask values
    cobalt_bid = soup.find("td", text="COBALT").find_next_sibling().text
    cobalt_ask = (
        soup.find("td", text="COBALT").find_next_sibling().find_next_sibling().text
    )

    # Find Aluminum Alloy bid and ask values
    aluminum_alloy_bid = soup.find("td", text="A.ALLY").find_next_sibling().text
    aluminum_alloy_ask = (
        soup.find("td", text="A.ALLY").find_next_sibling().find_next_sibling().text
    )

    # Find ZINC bid and ask values
    zinc_bid = soup.find("td", text="ZINC").find_next_sibling().text
    zinc_ask = soup.find("td", text="ZINC").find_next_sibling().find_next_sibling().text

    # Find NICKEL bid and ask values
    nickel_bid = soup.find("td", text="NICKEL").find_next_sibling().text
    nickel_ask = (
        soup.find("td", text="NICKEL").find_next_sibling().find_next_sibling().text
    )

    # Find Copper bid and ask values
    copper_bid = soup.find("td", text="COPPER").find_next_sibling().text
    copper_ask = (
        soup.find("td", text="COPPER").find_next_sibling().find_next_sibling().text
    )

    print(f"Currency: {currency_value}, Date: {date_value}")
    print(f"ALUM - Bid: {alum_bid}, Ask: {alum_ask}")
    print(f"LEAD - Bid: {lead_bid}, Ask: {lead_ask}")
    print(f"COBALT - Bid: {cobalt_bid}, Ask: {cobalt_ask}")
    print(f"Aluminum Alloy - Bid: {aluminum_alloy_bid}, Ask: {aluminum_alloy_ask}")
    print(f"ZINC - Bid: {zinc_bid}, Ask: {zinc_ask}")
    print(f"NICKEL - Bid: {nickel_bid}, Ask: {nickel_ask}")
    print(f"COPPER - Bid: {copper_bid}, Ask: {copper_ask}")

    pass


def grab_data_from_metal_market():
    today = datetime.date.today()
    MONDAY_CODE = 0
    SUNDAY_CODE = 6
    COPPER_CODE = "COPPER"
    NICKEL_CODE = "NICKEL"
    ZINC_CODE = "ZINC"
    ALUMINUM_ALLOY_CODE = "A.ALLY"
    COBALT_CODE = "COBALT"
    LEAD_CODE = "LEAD"
    ALUMINUM_CODE = "ALUM"
    if today.weekday() == MONDAY_CODE or today.weekday() == SUNDAY_CODE:
        print("LME dont update on Monday or Sunday")
        return
    
    copper_data = grab_metal_data(COPPER_CODE)
    save_to_db(copper_data)

    nickel_data = grab_metal_data(NICKEL_CODE)
    save_to_db(nickel_data)

    zinc_data = grab_metal_data(ZINC_CODE)
    save_to_db(zinc_data)

    aluminum_alloy_data = grab_metal_data(ALUMINUM_ALLOY_CODE)
    save_to_db(aluminum_alloy_data)

    cobalt_data = grab_metal_data(COBALT_CODE)
    save_to_db(cobalt_data)

    lead_data = grab_metal_data(LEAD_CODE)
    save_to_db(lead_data)

    aluminum_data = grab_metal_data(ALUMINUM_CODE)
    save_to_db(aluminum_data)


def grab_metal_data(metal_code):
    http = urllib3.PoolManager(cert_reqs="CERT_NONE")

    url = "https://www.metalsmarket.net/w_lmeCashSett.html"
    response = http.request("GET", url)
    soup = BeautifulSoup(response.data, "html.parser")

    # Find Currency And Date
    currency_td = soup.find("td", text="USD")
    date_td = currency_td.find_next_sibling()

    currency_value = currency_td.text.strip()
    date_value = date_td.text.strip()

    # Find METAL bid and ask values
    metal_bid = soup.find("td", text=metal_code).find_next_sibling().text
    metal_ask = (
        soup.find("td", text=metal_code).find_next_sibling().find_next_sibling().text
    )

    # assuming metal_data_dict["entry_date"] is a string like "03/02/2025"
    entry_date_str = date_value
    # entry_date_dt = datetime.strptime(entry_date_str, "%m/%d/%Y")
    entry_date_dt = datetime.strptime(entry_date_str, "%d/%m/%Y")
    # convert to Oracle DATE format
    oracle_date = entry_date_dt.strftime("%Y-%m-%d %H:%M:%S")

    metal_data_dict = {
        "entry_date": oracle_date,
        "data_source": "https://www.metalsmarket.net",
        "item_type": metal_code,
        "stlmnt_price": metal_bid,
        "ask_price": metal_ask,
        "currency": currency_value,
        "remarks": "Data from Scheduler",
    }

    return metal_data_dict


def save_to_db(metal_data_dict):
    with connection.cursor() as cursor:
        sql = """
        INSERT INTO XX_LME_PRICE_TREND 
        (ENTRY_DATE, DATA_SOURCE, ITEM_TYPE, STLMNT_PRICE, ASK_PRICE, CURRENCY, REMARKS, CREATION_DATE) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, SYSDATE)
        """
        cursor.execute(
            sql,
            (
                metal_data_dict["entry_date"],
                metal_data_dict["data_source"],
                metal_data_dict["item_type"],
                metal_data_dict["stlmnt_price"],
                metal_data_dict["ask_price"],
                metal_data_dict["currency"],
                metal_data_dict["remarks"],
            ),
        )

    connection.commit()  # Commit the transaction

    print("Data saved to the database. Date:", metal_data_dict["entry_date"])
