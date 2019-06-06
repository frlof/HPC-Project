import matplotlib.pyplot as plt
import math

def main():
    #print("test")
    X_axis = [1,2,4,8,16,32]
    resultsMed = []
    resultsVar = []
    #print("1 Thread")
    resultsMed.append(15.232127)
    resultsVar.append(0.046466)
    

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

    plt.plot(X_axis, resultsMed)
    plt.xlabel('Threads')
    plt.ylabel('Average time')
    plt.show()
    plt.plot(X_axis, resultsVar)
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