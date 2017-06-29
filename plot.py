import matplotlib.pyplot as plt
import numpy as np
from collections import OrderedDict
from operator import itemgetter

def normalize(lst):
    s = sum(x for x in lst)
    vals = [ float(x)/s for x in lst]
    return vals
#Plot frequent itemsets 
file = open("D:\Big_Data_Analytics\Project\Frequent_Itemset_2015_12.txt")
file1 = open("D:\Big_Data_Analytics\Project\Frequent_Itemset_2016_05.txt")
file2 = open("D:\Big_Data_Analytics\Project\Frequent_Itemset_2016_06.txt")
may_counts = file1.read()
june_counts = file2.read()
dec_counts = file.read()

print(dec_counts)
print(may_counts)
print(june_counts)

y = [1200, 27741, 7574, 2583618, 30, 524519, 20842, 535332,29,1,79983,591558,34508,73194,161,13311,312,5564,90035,199,17568,3]
x_labels = ['BM' , 'QB' , 'BrBr' , 'MM','BrS','QQ','BB','QM','SS','SB','BrQ','MQ','QBr','MBr','MS','BrB','BBr','BrQ','BrM','QS','MB','BS']
my_dict = {}
for i in range(len(y)):
    my_dict[x_labels[i]] = y[i]
dict_sort = OrderedDict(sorted(my_dict.items(), key=itemgetter(1), reverse = True))
print(dict_sort)
items = list(dict_sort.items())
keys = [x[0] for x in items[:6]]
values = [x[1] for x in items[:6]]
val = sum(x[1] for x in items[7:])
keys.append('Others')
values.append(val)

my_dict = {}
for i in range(len(values)):
    my_dict[keys[i]] = values[i]


plt.bar(range(len(my_dict)), my_dict.values())
x = np.arange(len(my_dict))
plt.xticks(x + 0.5, my_dict.keys(), rotation='vertical')
plt.title("Dec 2015 Frequent Items")
plt.show()

#Plot disputed payments
file = open("D:\Big_Data_Analytics\Project\Count_for_disputed_payments_2016_05.txt")
disputed_counts = file.read()
file1 = open("D:\Big_Data_Analytics\Project\Count_total_2016_05.txt")
total_counts = file1.read()
print(disputed_counts)
print(total_counts)

labels = 'Staten Island', 'Brooklyn', 'The Bronx', 'Queens' , 'Manhattan'
sizes = [2/1021, 452/222542, 310/180749, 3095/2057123, 5505/4412024]
sizes = normalize(sizes)
print(sizes)
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral' ,'red']

plt.pie(sizes, labels=labels, colors=colors,
        autopct='%f%%', shadow=True, startangle=90)
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
plt.title("June 2016 Disputed payments", y=1.04)
plt.show()

#Plot Tips
file = open("D:\Big_Data_Analytics\Project\Dec_2015_Tip.txt")
file1 = open("D:\Big_Data_Analytics\Project\May_2016_Tip.txt")
file2 = open("D:\Big_Data_Analytics\Project\June_2016_Tip.txt")
may_counts = file1.read()
june_counts = file2.read()
dec_counts = file.read()

print(dec_counts)
print(may_counts)
print(june_counts)

# The slices will be ordered and plotted counter-clockwise.
labels = 'Dec', 'May', 'June'
sizes = [0.3289897439652455, 0.2998299385868966, 0.6051633673060673]
sizes = normalize(sizes)
print(sizes)
colors = ['yellowgreen', 'gold', 'lightskyblue']

plt.pie(sizes, labels=labels, colors=colors,
        autopct='%f%%', shadow=True, startangle=90)
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
plt.title("Tip amount comparison per mile", y=1.04)
plt.show()

#Plot affluent neighbourhoods
file = open("D:\Big_Data_Analytics\Project\Affluent_2016_05.txt")
affluent_counts = file.read()
total_counts = file1.read()
print(affluent_counts)

labels = 'Brooklyn', 'Queens' , 'Manhattan'
sizes = [ 68.565493246009, 32.388972897522386, 47.676600985221675]
sizes = normalize(sizes)
print(sizes)
colors = ['yellowgreen', 'gold', 'lightcoral' ]

plt.pie(sizes, labels=labels, colors=colors,
        autopct='%f%%', shadow=True, startangle=90)
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
plt.title("Dec 2015 Tip Affluence", y=1.04)
plt.show()
