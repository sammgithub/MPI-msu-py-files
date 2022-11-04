import numpy as np
import cv2
import time
import pyscreenshot as Imagegrab #linux
startTime = time.time()

fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('video1.mov',fourcc, 2, (1280,666))


while True:
	img = Imagegrab.grab()
	img_np = np.array(img)
	
	frame = cv2.cvtColor(img_np, cv2.COLOR_BGR2RGB)
	
	#cv2.imshow("Screen", frame)
	out.write(frame)
	


	k = cv2.waitKey(1)
	if k == 27 or time.time()-startTime > 10 :
		break

out.release()
cv2.destroyAllWindows()