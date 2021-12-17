import timeout_decorator
import time
@timeout_decorator.timeout(5)
def mytest():
   print("Start")
   for i in range(1, 10):
      time.sleep(1)
      print("{} seconds have passed".format(i))
def main():
   try:
      mytest()
   except:
      print('abc')
if __name__ == '__main__':
   main()