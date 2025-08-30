from django.db import models
from django.contrib.auth.models import User
# Create your models here.
class Tag(models.Model):  ##manytomany
    name = models.CharField(max_length=50)
    def __str__(self):      ##This one is to make the content visible inside admin app
        return self.name

class Post(models.Model):
    post_title = models.CharField(max_length=60)
    post_content = models.TextField()
    published_date = models.DateField(auto_now=True)
    tags = models.ManyToManyField(Tag)
    post_image = models.ImageField(upload_to='post',default='post/1.png')
    
    def __str__(self):  ##this is to change the name inside the admin post columns
        return self.post_title
    
class Comment(models.Model): ##Onetomany
    comment = models.TextField()
    post = models.ForeignKey(Post,on_delete=models.CASCADE)
    user = models.ForeignKey(User,on_delete=models.CASCADE,null=True)