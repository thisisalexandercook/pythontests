from multiprocessing.pool import Pool
import time

def StringMerge(H,p,q,r):
    
    blankName = ['~~~~~~~~~~~~ ~~~~~~~~~~~~']
    leftArray = H[p:q+1] + blankName
    rightArray = H[q+1:r+1] + blankName
    i = 0
    j = 0

    for k in range(p,r+1):
        firstLeft, lastLeft = leftArray[i].split(" ")
        firstRight, lastRight = rightArray[j].split(" ")
        if lastLeft == lastRight:
            compareLeft = firstLeft
            compareRight = firstRight
        else:
            compareLeft = lastLeft
            compareRight = lastRight

        if len(compareRight) < len(compareLeft):
            loopLength = len(compareRight) 
        else:
            loopLength = len(compareLeft) 


        for x in range(loopLength):
            

            if compareLeft[x] < compareRight[x] or compareLeft == compareRight:
                H[k] = leftArray[i]
                i += 1
                break
            elif compareLeft[x] > compareRight[x]:
                H[k] = rightArray[j] 
                j += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) < len(compareRight)):
                H[k] = leftArray[i]
                i += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) > len(compareRight)):
                H[k] = rightArray[j] 
                j += 1
                break

    if len(leftArray) + len(rightArray) - 2 == len(name_list):
        pass
        #print(H)

def ParallelStringMerge(H,p,q,r,count):
    
    blankName = ['~~~~~~~~~~~~ ~~~~~~~~~~~~']
    leftArray = H[p:q+1] + blankName
    rightArray = H[q+1:r+1] + blankName
    i = 0
    j = 0

    for k in range(p,r+1):
        firstLeft, lastLeft = leftArray[i].split(" ")
        firstRight, lastRight = rightArray[j].split(" ")
        if lastLeft == lastRight:
            compareLeft = firstLeft
            compareRight = firstRight
        else:
            compareLeft = lastLeft
            compareRight = lastRight

        if len(compareRight) < len(compareLeft):
            loopLength = len(compareRight) 
        else:
            loopLength = len(compareLeft) 


        for x in range(loopLength):
            

            if compareLeft[x] < compareRight[x] or compareLeft == compareRight:
                H[k] = leftArray[i]
                i += 1
                break
            elif compareLeft[x] > compareRight[x]:
                H[k] = rightArray[j] 
                j += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) < len(compareRight)):
                H[k] = leftArray[i]
                i += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) > len(compareRight)):
                H[k] = rightArray[j] 
                j += 1
                break

    if len(leftArray) + len(rightArray) - 2 == count:
        return (H)

def FinalParallelStringMerge(leftArray,rightArray,p,r):
    
    i = 0
    j = 0

    for k in range(p,r+1):
        firstLeft, lastLeft = leftArray[i].split(" ")
        firstRight, lastRight = rightArray[j].split(" ")
        if lastLeft == lastRight:
            compareLeft = firstLeft
            compareRight = firstRight
        else:
            compareLeft = lastLeft
            compareRight = lastRight

        if len(compareRight) < len(compareLeft):
            loopLength = len(compareRight) 
        else:
            loopLength = len(compareLeft) 


        for x in range(loopLength):
            

            if compareLeft[x] < compareRight[x] or compareLeft == compareRight:
                outputArray[k] = leftArray[i]
                i += 1
                break
            elif compareLeft[x] > compareRight[x]:
                outputArray[k] = rightArray[j] 
                j += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) < len(compareRight)):
                outputArray[k] = leftArray[i]
                i += 1
                break
            elif (x == loopLength - 1 and len(compareLeft) > len(compareRight)):
                outputArray[k] = rightArray[j] 
                j += 1
                break



def StringMergeSort(Z,p,r):
    if p < r:
        q = (p+r) // 2
        StringMergeSort(Z,p,q,)
        StringMergeSort(Z,q+1,r,)
        StringMerge(Z,p,q,r,)

def ParallelStringMergeSort(Z,p,r,count):
    
    result = []
    
    if p < r:
        q = (p+r) // 2
        ParallelStringMergeSort(Z,p,q,count)
        ParallelStringMergeSort(Z,q+1,r,count)
        result = ParallelStringMerge(Z,p,q,r,count)

    return result

if __name__ == '__main__':
    with Pool() as pool:

    
        with open('names.txt') as f:
            lines = f.readlines()

        name_list = []

        for x in lines[0:]:
            name_list.append(x.replace("\n",""))

        outputArray = name_list
        chunk_size = len(name_list)//2
        list_chunked = [(name_list[i:i + chunk_size],0,(chunk_size - 1),chunk_size) for i in range(0, len(name_list), chunk_size)]

        parallelStart = time.time()

        output = pool.starmap(ParallelStringMergeSort, (list_chunked))
        FinalParallelStringMerge(output[0] + ['~~~~~~~~~~~~ ~~~~~~~~~~~~'], output[1] + ['~~~~~~~~~~~~ ~~~~~~~~~~~~'],0,len(name_list)-1)

        parallelEnd = time.time()
        parallelRunTime = parallelEnd - parallelStart
        print("parallel runtime = ", parallelRunTime)


    with open('names.txt') as f:
        lines = f.readlines()

    name_list1 = []

    for x in lines[0:]:
        name_list1.append(x.replace("\n",""))

    sequentialStart = time.time()
    StringMergeSort(name_list1,0,len(name_list1)-1)
    sequentialEnd = time.time()
    sequentialRunTime = sequentialEnd - sequentialStart
    print("sequential runtime = ", sequentialRunTime)


        

