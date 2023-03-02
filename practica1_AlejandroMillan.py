from multiprocessing import Process, Manager
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random, randint

N = 10	#Cantidad de veces que va a producir cada productor
K = 5	#Capacidad de los buffers de cada consumidor (suponemos que es K>0)
NPROD = 4	#Numero de productores que vamos a tener


def delay(factor = 3):
	"""
	Definimos la funcion que simulara que los procesos tarden mas tiempo en ejecutarse
	"""
	sleep(random()/factor)
	

def add_value(storage, mutex, value):
	"""
	Definimos la funcion que hace que cada productor añada un valor dado a su buffer
	"""
	mutex.acquire()
	try:
		found = False
		i = 0
		while not found and i < len(storage):	#Añadimos el valor en el primer hueco vacio del buffer
			if storage[i] == -2:
				storage[i] = value
				found = True
			i += 1
		delay(6)
	finally:
		print(f'{current_process().name} ha almacenado {value} en su buffer: {list(storage)}') #Indicamos el valor guardado y la situacion en la que queda el buffer
		mutex.release()


def get_value(storage, almacen, mutex, index):
	"""
	Definimos la funcion con la que merge añade un valor de un productor al almacen general y extrae el valor del buffer del productor
	"""
	mutex.acquire()
	try:
		value = storage[0]	#Añadimos el valor al almacen principal  
		almacen.append(value)
		delay()
		for i in range(len(storage)-1):	#Eliminamos el valor del buffer del productor
			if storage[i] != -2:
				storage[i] = storage[i+1]
		storage[-1] = -2
		print(f'merge ha extraido {value} del productor {index}, lo guarda en {almacen} y el buffer del productor queda: {list(storage)}')	#Indicamos el valor  extraido, de que productor, la situacion actual del buffer general y la del buffer del productor correspondiente
	finally:
		mutex.release()


def producer(storage, non_empty, empty, mutex, lastValue):
	"""
	Definimos la funcion de los procesos productores
	"""
	for v in range(1, N):
		print(f'{current_process().name} produciendo')	#Indicamos que el proceso comienza a producir
		delay(6)
		empty.acquire()
		lastValue = lastValue + randint(1,5) #Decidimos que valor vamos a añadir, este sera el ultimo valor añadido en el buffer, incrementado entre 1 y 5 unidades de manera aleatoria
		add_value(storage, mutex, lastValue)
		non_empty.release()
	empty.acquire()	#Una vez que acaba de producir añadimos -1 al buffer para saber que ha acabado
	add_value(storage, mutex, -1)
	print(f'{current_process().name} ha acabado')	#Indicamos que el proceso ha terminado
	non_empty.release()
		

def consumer(storageLst, almacen, non_emptyLst, emptyLst, mutexLst):
	"""
	Definimos la funcion del proceso consumidor
	"""
	for i in range(len(storageLst)):	#Ponemos en espera los semaforos de todos los procesos
		non_emptyLst[i].acquire()
		
	while noTerminado(storageLst):	#merge va a funcionar mientras queden elementos en alguno de los buffers de los productores
		print(f'merge buscando valor')
		posibles = []	#Para decidir que valor vamos a escoger miramos todos los primeros elementos de cada buffer y cogemos el minimo de ellos, que no sea -1 ni -2
		for storage in storageLst:
			posibles.append(storage[0])
		(valor, index) = minimo(posibles)
		get_value(storageLst[index], almacen, mutexLst[index], index)
		emptyLst[index].release()
		non_emptyLst[index].acquire()
		delay()
	print(f'Ya han acabado todos los productores')	#Indicamos que ya han acabado todos los procesos productores
	print(f'El buffer general es: {almacen}')	#Mostramos como queda el buffer de merge
	for i in range(NPROD):
		print(f'El buffer del productor {i} queda: {list(storageLst[i])}') #Indicamos como quedan los buffers de cada uno de los productores


def minimo(valLst):
	"""
	Definimos una función que nos de el menor valor que podemos añadir al buffer general, junto el indice del productor al que pertenece
	"""
	valor = max(valLst)
	i = valLst.index(valor)
	for j in range(len(valLst)):
		if valLst[j] > 0 and valLst[j] < valor:
			valor = valLst[j]
			i = j
	return (valor, i)
		
		


def noTerminado(bufferLst):
	"""
	Definimos una funcion que nos avise cuando han terminado todos los productores
	"""
	b = False
	for storage in bufferLst:
		if storage[0] != -1:
			b = True
	return b
	
def main():
	"""
	Definimos la funcion main que ejecuta el programa
	"""

	storageLst = [Array('i', K) for j in range(NPROD)]	#Esta es la lista con los buffer de cada productor
	
	for storage in storageLst:
		for j in range(K):
			storage[j] = -2 	#Inicilizamos todos los buffer para que esten vacios
		
	manager = Manager()
	almacen = manager.list()	#Creamos el buffer ilimitado donde el consumidor guardara los elementos
	
	#Creamos todos los semáforos que necesitamos
	
	non_emptyLst = [Semaphore(0) for i in range(NPROD)]
	emptyLst = [BoundedSemaphore(K) for i in range(NPROD)]
	mutexLst = [Lock() for i in range(NPROD)]
	
	#Creamos los procesos productores
	prodLst = [Process(target=producer, name=f'productor {i}', args=(storageLst[i], non_emptyLst[i], emptyLst[i], mutexLst[i], randint(1,5))) for i in range(NPROD)]
	
	#Creamos el proceso consumidor
	merge = Process(target=consumer, name=f'merge', args=(storageLst, almacen, non_emptyLst, emptyLst, mutexLst))
	
	for p in prodLst+[merge]:
		p.start()	#Iniciamos todos los procesos
	
	for p in prodLst+[merge]:
		p.join()	#Esperamos a que acaben
	
	
if __name__ == '__main__':
	main()
