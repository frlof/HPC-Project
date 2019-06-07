import matplotlib.pyplot as plt
import math

def main():

    #2 198310 195760 197370 196300 196690
    #4 69260 69170 68970 69390 71280
    #8 33800 33750 33500 33760 33640
    #16 20520 20430 20720 20610 20700
    #32 21690 21730 21550 21490 21640
    #print("test")
    X_axis = [2,4,8,16,32]
    resultsMed = []
    resultsVar = []

    #print("2 threads")
    arr = [198.31, 195.76, 197.37, 196.30, 196.69]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("4 threads")
    arr = [69.26, 69.17, 68.97, 69.39, 71.28]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("8 threads")
    arr = [33.80, 33.75, 33.50, 33.76, 33.64]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("16 threads")
    arr = [20.52, 20.43, 20.72, 20.61, 20.70]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("32 threads")
    arr = [21.69, 21.73, 21.55, 21.49, 21.640]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)


    
    '''
    #print("2 Thread")
    resultsMed.append(7.727781)
    resultsVar.append(0.041065)

    #print("4 Thread")
    resultsMed.append(4.220584)
    resultsVar.append(0.028162)

    #print("8 Thread")
    resultsMed.append(2.396208)
    resultsVar.append(0.005147)

    #print("16 Thread")
    resultsMed.append(1.243089)
    resultsVar.append(0.002018)

    #print("32 Thread")
    resultsMed.append(0.622770)
    resultsVar.append(0.000461)
    '''
    plt.plot(X_axis, resultsMed, label="10 GB dataset")
    plt.legend()
    plt.xlabel('Threads')
    plt.ylabel('Average time (seconds)')
    plt.show()
    plt.plot(X_axis, resultsVar, label="10 GB dataset")
    plt.legend()
    plt.xlabel('Threads')
    plt.ylabel('Standard deviation')
    plt.show()


def average(arr):
    temp = 0
    for i in range(0,len(arr)):
        temp += arr[i]
    temp /= 5
    return temp

def variance(arr, med):
    temp = 0
    for i in range(0,len(arr)):
        temp += (arr[i] - med)**2
    #temp /= 5
    return temp

main()