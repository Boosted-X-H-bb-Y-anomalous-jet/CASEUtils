import tensorflow as tf
import os
import numpy as np
import h5py

def append_VAE_loss_to_h5(fout):
    model_name = os.getcwd()+"/jrand_autoencoder_m2500.h5" #Have to give absolute path here ._.
    model = tf.keras.models.load_model(model_name)
    
    
    Y_idx = fout["Y_idx"][:]
    fsignal1 = fout["j1_images"][:,:,:]
    fsignal2 = fout["j2_images"][:,:,:]
    fsignalY = [fsignal1[i] if Y_idx[i] == 0 else fsignal2[i] for i in range(len(Y_idx))]


    fsignalY = np.expand_dims(fsignalY, axis = -1)
    reco_signalY = model.predict(fsignalY, batch_size = 1)

    vae_loss =  np.mean(np.square(reco_signalY - fsignalY), axis=(1,2)).reshape(-1)
    fout.create_dataset("Y_vae_loss", data=vae_loss, chunks = True, maxshape=(None,), compression = 'gzip')
    print("Appended Y_vae_loss to file")

def append_Y_idx(fout):
    hbb_signal_1 = fout['jet1_extraInfo'][:,-2]
    hbb_signal_2 = fout['jet2_extraInfo'][:,-2]

    #H-cand jet is the one with higher bb-tag score, Y is the other one
    Y_idx = [1 if signal_1 > signal_2 else 0 for signal_1, signal_2 in zip(hbb_signal_1, hbb_signal_2)]
    fout.create_dataset("Y_idx", data=Y_idx, chunks = True, maxshape=(None,), compression = 'gzip')
    print("Appended Y_idx to file")

with h5py.File("with_jet_images.h5","a") as fout:
    append_Y_idx(fout)
    append_VAE_loss_to_h5(fout)
