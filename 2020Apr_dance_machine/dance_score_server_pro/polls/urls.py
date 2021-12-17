from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^score', views.score, name='score'),
]
