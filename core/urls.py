from django.urls import path

from core import views


# from .views import say_hello

urlpatterns = [
    # path("", views.home, name="home"),
    path("test/", views.test, name="home"),
    # path("insert/", views.insert, name="insert"),
]
