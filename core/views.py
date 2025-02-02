from datetime import datetime
import re
from django.shortcuts import render
from django.http import HttpResponse

from core.models import LMEPriceTrend
from django.db import connection

def get_lme_price_trend():
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM XX_LME_PRICE_TREND")
        columns = [col[0] for col in cursor.description]  # Get column names
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]  # Convert to dictionary
    return results

def insert_lme_price_trend(entry_date, data_source, item_type, stlmnt_price, ask_price, currency):
    with connection.cursor() as cursor:
        sql = """
        INSERT INTO XX_LME_PRICE_TREND 
        (ENTRY_DATE, DATA_SOURCE, ITEM_TYPE, STLMNT_PRICE, ASK_PRICE, CURRENCY, CREATION_DATE) 
        VALUES (%s, %s, %s, %s, %s, %s, SYSDATE)
        """
        cursor.execute(sql, (entry_date, data_source, item_type, stlmnt_price, ask_price, currency))
      

def test(request):
    insert_lme_price_trend(
        '30-FEB-25',  # ENTRY_DATE
        'https://www.lme.com/',  # DATA_SOURCE
        'LME Aluminum',  # ITEM_TYPE
        2500.00,  # STLMNT_PRICE
        2501.00,  # ASK_PRICE
        'USD'  # CURRENCY
    )

    data = get_lme_price_trend()
    for row in data:
        print(row)  # Print each row
    context = {
        "test": "something_cool",
    }
    # return render(request, "core/something_cool.html", context=context)
    return HttpResponse("Hello, World!", status=200)
