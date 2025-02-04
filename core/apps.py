from django.apps import AppConfig
from apscheduler.schedulers.background import BackgroundScheduler
from core.scheduler import grab_data_from_metal_market, scrap_data_from_metal_market, your_job_function
import threading

class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'

    def ready(self):
        if threading.current_thread().name == "MainThread":
            start_scheduler()


def start_scheduler():
    scheduler_instance = BackgroundScheduler()
    # scheduler_instance.add_job(your_job_function, "interval", minutes=1)
    scheduler_instance.add_job(grab_data_from_metal_market, "interval", minutes=1)
    # scheduler_instance.add_job(scrap_data_from_metal_market, "cron", hour=0, minute=5)

    scheduler_instance.start()
