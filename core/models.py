from django.db import models
from django.db import models

class LMEPriceTrend(models.Model):
    entry_date = models.DateTimeField()
    data_source = models.CharField(max_length=255)
    item_type = models.CharField(max_length=255)
    stlmnt_price = models.DecimalField(max_digits=10, decimal_places=2)
    ask_price = models.DecimalField(max_digits=10, decimal_places=2)
    creation_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False  # This means Django won't manage this table
        db_table = "XX_LME_PRICE_TREND"


