import matplotlib.pyplot as plt
import math

def main():

    #10 21720 21780 21980 21840 21510
    #20 38890 38870 39010 39210 39690
    #40 83690 85170 84440 84060 84440

    #print("test")
    X_axis = [10,20,40]
    resultsMed = []
    resultsVar = []

    #print("10 gig")
    arr = [21.72, 21.78, 21.98, 21.84, 21.51]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("20 gig")
    arr = [38.89, 38.87, 39.01, 39.21, 39.69]
    avg = average(arr)
    var = variance(arr, avg)
    resultsMed.append(avg)
    resultsVar.append(var)

    #print("40 gig")
    arr = [83.69, 85.17, 84.44, 84.06, 84.44]
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
    plt.plot(X_axis, resultsMed, label="32 Threads")
    plt.legend()
    plt.xlabel('Data Size (GB)')
    plt.ylabel('Average time (seconds)')
    plt.show()
    plt.plot(X_axis, resultsVar, label="32 Threads")
    plt.legend()
    plt.xlabel('Data Size (GB)')
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