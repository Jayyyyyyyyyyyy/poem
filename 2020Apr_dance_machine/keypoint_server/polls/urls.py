from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^keypoints', views.keypoints, name='keypoints'),
]
