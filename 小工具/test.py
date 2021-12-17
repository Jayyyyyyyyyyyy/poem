import os
from PIL import Image
import numpy as np
import tensorflow as tf
from tensorflow.python.keras.models import load_model


def img_preprocess(path):
    img = Image.open(path)
    res = img.resize((512, 512))
    # format convert
    res = np.asarray(res)
    if res.shape[-1] == 4:
        res[:, :, :-1]
    else:
        res
    return res, img


def load_mymodel(structure_path, weights_path):
    from tensorflow.python.keras.models import load_model
    with open(structure_path, "r") as f:
        json_str = f.read()
    f.close()
    # Structure
    model = tf.keras.models.model_from_json(json_str)
    # Weights
    model.load_weights(weights_path)
    # base_learning_rate = 0.0001
    model.compile(optimizer=tf.keras.optimizers.Adam(),
                  loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                  metrics=['accuracy'])
    return model


structure_file = '/Users/jiangcx/Documents/model2.json'
weight_file = '/Users/jiangcx/Documents/model2.hdf5'
model = load_mymodel(structure_file, weight_file)

root_path = '/Users/jiangcx/Documents/test_imgs/'
# root_path =  '/Users/jiangcx/Desktop/new_image_quality_data'

res_path = '/Users/jiangcx/Documents/img_quality_test/'
mydict = {0: 'high', 1: 'low', 2: 'normal'}
yes = 0
no = 0
total = 0
for x in os.listdir(root_path):
    folder = os.path.join(root_path, x)
    for file in os.listdir(folder):
        total = total + 1
        image_path = os.path.join(folder, file)
        print(image_path)
        img, org_img = img_preprocess(image_path)
        img = img.reshape(-1, 512, 512, 3)
        res = model.predict(img)
        res = np.argmax(res)
        res = mydict[res]
        if x == res:
            newfile = '1+' + x + '-' + res + '_' + file
            yes += 1
        else:
            newfile = '2+' + x + '-' + res + '_' + file
            no += 1
        file_name = os.path.join(res_path, newfile)
        org_img.save(file_name)
print(yes / total)
#     abc = "frame{}:{}.jpg".format(m, s)
#     outfile = os.path.join(path2,abc)
#     shutil.copy(infile, outfile)
