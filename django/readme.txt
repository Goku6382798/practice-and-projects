Commands and use:

django-admin startproject <name-of-choice> = to create project
python3 manage.py runserver = To run server
apps in django = python3 manage.py startapp posts

CLIENT ---> DJANGO-POWERED SITE ---> ROOT_URLCONF = BLOG.URLS ---> URLS.PY ---> URLS.PY ---> VIEW

workflow using terminal in django(To create table inside db follow step by step) 

    Add/Changes to Model(It is also used while creating model) --> Makemigrations
    Command to apply the changes(mean deploy the changes inside database) --> python3 manage.py sqlmigrate post 0001
    Final change to make changes inside database(This one is to create table inside database) :- python3 manage.py migrate

Complete workflow
    Add/Changes to Model ---> makemigrations ---> Generates migration file ---> migrate ---> Executes Sql ---> DB

How to make changes :-
    make the changes inside model.
        1. python3 manage.py makemigrations  (It generates a new migrations files)
        2. python3 manage.py showmigrations (to check all the changes)
        3. python3 manage.py migrate (Apply the changes we created)

How to check edit the data base(using terminal):-
Behind the scene the django ORM used the sql query and Inserted the data inside the db
    1. python3 manage.py shell
        from posts.models import Post
        post1 = Post(post_title='First post title',post_content='First post content')
        post1.save()
        Post.objects.all() **(returns data in query set)**
        data = Post.objects.all()
        data[0].post_title
    2. Another way is simple using a query
        Post.objects.create(post_title='Second post title',post_content='Post content of second post')
    3. Now how to query data inside database
        from django.db import connection
        connection.queries
                **** For all the query reference in django look for django queryset api refernce ****
        

    4. Get() Filter() Exclude()
        Same again simply import the model
        from posts.models import Post
        Post.objects.get(id=2)
        sam = Post.objects.get(id=2)
        sam.id
        sam.name
        Post.objects.filter(age=15)
        Post.objects.exclude(age=15)
        allData = Post.objects.all()
        allData.order_by('-id')
        allData.values()
        Post.objects.all().count()
        Post.objects.filter(age=18).count()
        allData.contains(simran)
        data = Post.objects.filter(student_class=12) & Post.objects.filter(age=18)  (And condition)
        data = Post.objects.filter(student_class=12) | Post.objects.filter(age=18)  (Or condition)
        Q object in dango shell
        from django.db.models import Q
        Post.objects.filter(Q(student_class=12) & Q(age=18))
        Post.objects.filter(Q(student_class=12) | Q(age=18))
        Post.objects.filter(~Q(name='Neha'))
        Post.objects.all()[:2]
        Post.objects.all()[2:5]
        Post.objects.all().order_by('-id')[:4]
    5.Field Lookups
        from students.models import Post
        Post.objects.filter(age >= 18)
        Post.objects.filter(age__gte=18)
        Post.objects.filter(name__isstartwith='s')
        Post.objects.filter(id__in=[1,3,4])
    6. from django.db import connection
        connection.queries  //(this is to check if a query run behind the scene or not sql query )
    7. Updating Single row and Multiple row
        simar = Students.object.get(id=1)
        simar.age = 20
        simar.save()
        all_students = Students.objects.all()
        all_students.update(name='Student')
        Students.objects.filter(age__gte=18).update(name='Adult')
        Students.objects.get(id=1).update(name='like')
        Students.objects.filter(id=2)
        Students.objects.filter(id=2).update(name='Adult')
    8. Deleting Single and multiple row
        from students.model import Students
        d1 = Students.objects.get(id=1)
        d1.delete()
        d2 = Students.objects.filter(age__gte=18)
        d2.delete()
        d3 = Students.objects.all()
        d3.delete()
    9.Fetching & Rendering Data In Templates
        Go inside app then open views.py 
        Get inside application all_posts = Post.objects.all()
        print(all_posts)
    10.Deep Diving into aggregation
        from django.db.model import Max, Min, Sum
        Students.objects.filter(age__gte=18).aggregate(Max('age'))
        Students.objects.aggregate(Max('age'))
        Students.objects.aggregate(Max('age'))['age__max']

-----------------------------------------------------------------------------

Admin panel inside django :- simply use the website url/admin
    how to create the user and password :- python3 manage.py createsuperuser
    How to register model inside admin django :- inside admin.py import models then add admin.site.register(XYZ)

forms in django :-
    In django we use get and post
    Get sent data through url/Request data from server/Retrieving data from a database
    Post sent data through body/Send data to server/Creating/updating data to a database

CSRF ATTACK(CSRF VERIFICATION FAILED):-
    In simple words its a tokken required by the main page so no any unauthentic website can delete or edit our data or account
    How to remove CSRF Verification failed :- Simply add {% csrf_token %} inside the form example inside Students/teachers
    
Difference between modelform and form :-
| Feature               | `forms.Form`                               | `forms.ModelForm`                           |
| --------------------- | ------------------------------------------ | ------------------------------------------- |
| **Link to model**     | ❌ No                                      | ✅ Yes                                       |
| **Field definitions** | Must define all manually                   | Auto-generated from model fields            |
| **Validation**        | Manual / custom                            | Uses model field validation automatically   |
| **Save to DB**        | Must code manually                         | `form.save()` handles it                    |
| **Use case**          | Any form not bound to a model (login, etc) | CRUD forms for models (create/edit objects) |


In modelform and form the way we add the clean method/delete method/get is same as forms.form 


                Request                                         MIDDLEWARE1 MIDDLEWARE2 MIDDLEWARE3                         MIDDLEWARE3 MIDDLEWARE2 MIDDLEWARE1         RESPONSE                                                                         
CLIENT/BROWSER ------------------------------> DJANGO WEBSITE -----------------------------------------------------> VIEW -----------------------------------------------------------> CLIENT/BROWSER

*MIDDLEWARE can also sent the response like if MIDDLEWARE2 send a response it will pass through MIDDLEWARE1
*We can also create CUSTOM MIDDLEWARE PYTHON FUNCTION OR CLASS

*MiddleWare Hooks we can only use them inside class based MiddleWare thats why mostly class based are used

*In django we have a single user table in the database where we can store superuser staffuser and normaluser which is auth_user 

*Django gives us built in login and logout views but not the register one

* RELATIONSHIPS

    ONE TO ONE :- One Row Of Table(A) Connected to One Row Of Another Table(B) :------ Husband - Wife
    MANY TO ONE :- Multiple Rows Of Table (A) COnnected to one row of another table (B) :---------------- Comment - Post
    MANY TO MANY :- Row Of Table(A) Connected To Multiple Rows Of Another Table(B) And Each Row in Second Table(B) Can Also be Linked to Multiple Rows Of Table(B) :--------- POSTS-TAGS

CBVs :- CLASS BASED VIEW:-
    BASE VIEWS                          GENERIC VIEWS
        VIEW                                DISPLAY VIEW
        TEMPLATEVIEW                            LISTVIEW,DETAILVIEW
        REDIRECTVIEW                        EDITING VIEW
                                                FORMVIEW,CREATEVIEW,UPDATEVIEW,DELETEVIEW