import struct, binascii, os, time
import pandas as pd
# import numpy as np
import matplotlib
import matplotlib.pyplot as plt
Float = os.getcwd() + '/log/2018 01 22 0000 (Float).DAT'

def Get_tag_names():
  Tagname = Float[0:-11]+'(Tagname).DAT'
  try:
    with open(Tagname, 'r') as fi:
      fi.read(10)
      names = tuple(fi.read().split()[1:][::3])
      return names
  except:
    return tuple(x for x in xrange(10000))

def read_data(file):
  data_list_temp=[]
  flag = False
  print ('read_data')
  with open(os.getcwd() + '/' + Float[-27:-12] + '.csv', "a") as filik:
    while True:
      read = binascii.hexlify(file.read(1))
      if read.lower() == '0D'.lower():
        flag = True
      elif read.lower() == '20'.lower() and flag==True:
        # frm_reader('<19s3s2sdcci', , temp_names)
        data_list_temp.append(list(struct.unpack('<19s3s2sdcci', file.read(38))))
      elif read.lower() == "1A".lower():
        break
    # a = max(x[2] for x in (data_list_temp[:500]))
    print ('read_data DONE!')
    return data_list_temp#, a

def conver_date(listik):
  names = Get_tag_names()
  listik = list(listik)
  for x in xrange(len(listik)):
    listik[x][2] = names[int(listik[x][2])]
    # listik[x][0] = time.strptime(listik[x][0][0:16], '%Y%m%d%H:%M:%S')
    listik[x][0] = '%s-%s-%s %s:%s:%s' % (listik[x][0][:4],
                                          listik[x][0][4:6],
                                          listik[x][0][6:8],
                                          listik[x][0][8:10],
                                          listik[x][0][11:13],
                                          listik[x][0][14:16])
  return listik

def printer(s, name):
  with open(os.getcwd() + '/' + name + '.csv', "w") as filik:
    print >> filik, 'Datetime, Milisecond, Tag, Value, Index'
    for t in xrange(len(s)):
      print >> filik, '%s-%s-%s %s:%s:%s, ms:%s, %s, %.3f, %s' % (s[t][0][:4],  # year
                                                                  s[t][0][4:6],  # month
                                                                  s[t][0][6:8],  # day
                                                                  s[t][0][8:10],  # hour
                                                                  s[t][0][11:13],  # minut
                                                                  s[t][0][14:16],  # second
                                                                  s[t][0][16:],  # msecond
                                                                  s[t][2],  # Tagname
                                                                  s[t][3],  # Value
                                                                  s[t][6]) #Index

def getkey(item):
  return item[2]

def get_dick(datalist):
  d = {'Datetime':[x[0] for x in datalist],
       'Tag':[x[2] for x in datalist],
       'Value':[x[3] for x in datalist],
       'Index':[x[-1] for x in datalist]}
  return d

def get_min_max_tag_index(tags, name):
  unic = [x for x in xrange(len(tags)) if name in tags[x]]
  return unic[0], unic[-1]

start_time = time.time()
print ("Start: %s" % start_time)
with open(Float, 'rb') as fi:
  data_list = read_data(fi)#, maximum = read_data(fi)
sortedlist = sorted(conver_date(data_list), key=getkey)
data = get_dick(sortedlist)
print ('<Reading time %.3f seconds. Drop to disk>' % (time.time() - start_time))
# print set(data['Tag'])
min, max = get_min_max_tag_index(data['Tag'], '[N3]VKT[3].TEMPERATURE')
df = pd.DataFrame(data['Value'][min:max], index=[pd.Timestamp(x) for x in data['Datetime'][min:max]])
df.plot(kind='line')
plt.show()
printer(sortedlist, Float[-27:-12])
print ('<Total execution time %.3f seconds>' % (time.time() - start_time))



#~~~~~~~~~~~ 3.5 - 4 sec   for t in xrange(len(s)):~~~~~~~~~~~~
# def printer(s, name, maximus , part='', z=0, sep=0):
#   print "Drop to disk " + str(z)
#   temp_names = Get_tag_names()
#   with open(os.getcwd() + '/' + name + part +'.csv', "w") as filik:
#     for t in xrange(len(s)):
#       print >> filik, '%s-%s-%s %s:%s:%s, ms:%s, %s, %.3f, %s' % (s[t+sep][0][:4],  # year
#                                                                   s[t+sep][0][4:6],  # month
#                                                                   s[t+sep][0][6:8],  # day
#                                                                   s[t+sep][0][8:10],  # hour
#                                                                   s[t+sep][0][11:13],  # minut
#                                                                   s[t+sep][0][14:16],  # second
#                                                                   s[t+sep][0][16:],  # msecond
#                                                                  temp_names[int(s[t+sep][2])],  # Tagname
#                                                                  int(s[t+sep][3]),  # Value
#                                                                   s[t+sep][6])
#       if t > 999999 and int(s[t][2]) == 0:
#         z+=1
#         printer(s, name, maximus, ' Part ' + str(z), z, t)

#~~~~~~~~~~~ 3.2 - 3.5 sec   for t in s:~~~~~~~~~~~~
# def printer(s, name , i='', j=0):
#   x = 0
#   print "Drop to disk"
#   temp_names = Get_tag_names()
#   with open(os.getcwd() + '/' + name + i[:-1] +'.csv', "w") as filik:
#     for t in s:
#       print >> filik, '%s-%s-%s %s:%s:%s, ms:%s, %s, %.3f, %s' % (t[0][:4],  # year
#                                                                   t[0][4:6],  # month
#                                                                   t[0][6:8],  # day
#                                                                   t[0][8:10],  # hour
#                                                                   t[0][11:13],  # minut
#                                                                   t[0][14:16],  # second
#                                                                   t[0][16:],  # msecond
#                                                                  temp_names[int(t[2])],  # Tagname
#                                                                  int(t[3]),  # Value
#                                                                   t[6])
#       x += 1
#       if x > 999999:
#         i = ' part ' + str((len(s) / 1000000) * 10)
#         printer(s, name, i, x + j)

# listik[x][0] = time.strptime(listik[x][0][0:16], '%Y%m%d%H:%M:%S'