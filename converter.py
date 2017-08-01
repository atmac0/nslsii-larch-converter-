import h5py
import numpy as np
import sys, os

varlenstring = h5py.special_dtype(vlen=str)
dt = h5py.special_dtype(vlen=bytes)
tfenum = h5py.special_dtype(enum = ('i', {"FALSE":0, "TRUE":1}))

def add_from_template(h5file):
    template = h5py.File(os.path.dirname(os.path.realpath(__file__)) + '/config/Template.h5','r')
    template.copy('xrfmap/config', h5file['xrfmap'])
    template.close()
    
def add_larch_datasets(h5file):
    try:
        work = h5file.create_group("xrfmap/work/")
    except:
        pass

def populate_sum_raw(sum_raw, counts, I0, sum_limit_data):
    for index in range(0,sum_limit_data.shape[0]):
        if sum_limit_data[index][0] == -1:
            pass
        else:
            sum_raw[:,:,index] = np.sum(counts[:,:,sum_limit_data[index][0]:sum_limit_data[index][1]],axis=2)
    for index in range(0,I0.shape[2]):
        sum_raw[:,:,index] = I0[:,:,index]
    print("Creation of sum_raw completed.")

def populate_det_raw(det_raw, det_counts, I0, det_limit_data,detNum,NDETECTORS):
    for index in range(detNum-1,det_limit_data.shape[0]-1,NDETECTORS):
        det_raw[:,:,index] = np.sum(det_counts[:,:,det_limit_data[index][0]:det_limit_data[index][1]],axis=2)
    print('Creation of det_raw for detector ', detNum, ' out of ', NDETECTORS, ' complete')

def get_ndetectors(h5file):
    ndetectors = 1
    try:
        while(h5file['xrfmap/det%d' %ndetectors]):
            ndetectors +=1
    except:
        return (ndetectors-1)

def add_larch_attributes(h5file):    
    NDETECTORS = get_ndetectors(h5file)
    xrfmap = h5file['xrfmap']
    
    for detector in range(1,NDETECTORS+1):
        det = h5file['xrfmap/det%i' %(detector)]
        det.attrs.create(name='desc',data='mca%i'%detector,shape=None,dtype=h5py.special_dtype(vlen=str))
        det.attrs.create(name='type',data='mca detector', shape=None,dtype=h5py.special_dtype(vlen=str))

    h5file['xrfmap/detsum'].attrs.create(name='desc',data='sum of detectors',shape=None,dtype=h5py.special_dtype(vlen=str))
    h5file['xrfmap/detsum'].attrs.create(name='type',data='virtual mca', shape=None,dtype=h5py.special_dtype(vlen=str))

    h5file['xrfmap/roimap'].attrs.create(name='desc',data='ROI data',shape=None,dtype=h5py.special_dtype(vlen=str))
    h5file['xrfmap/roimap'].attrs.create(name='type',data='roi maps', shape=None,dtype=h5py.special_dtype(vlen=str))
    
    try:
        xrfmap.attrs.create(name='Map_Folder',data='T:/xas_user/2016.2/Miller/Maps/Root4_Root4nuetral.001_rawmap',shape=None,dtype=h5py.special_dtype(vlen=str))
        
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Beamline',data='NSLSII',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Dimension',data='2',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Last_Row',data=1,shape=None,dtype='i')
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Start_Time',data='date',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Stop_Time',data='date',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Title',data='Epics Scan Data',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Type',data='Map',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name='Version',data='1',shape=None,dtype=h5py.special_dtype(vlen=str))
    except RuntimeError:
        pass
    try:
        xrfmap.attrs.create(name = 'N_Detectors',data = NDETECTORS,shape = None,dtype = 'i')
    except RuntimeError:
        pass

    
def create_roi_limit(h5file,sum_limit_data):
    try:
        roi_limit = h5file.create_dataset("xrfmap/detsum/roi_limits", shape=(sum_limit_data.shape[0],2), dtype='i')
    except RuntimeError:
        del h5file["xrfmap/detsum/roi_limits"]
        roi_limit = h5file.create_dataset("xrfmap/detsum/roi_limits", shape=(sum_limit_data.shape[0],2), dtype='i')

    roi_limit[:,0] = [sum_limit_data[y][0] for y in range(0,sum_limit_data.shape[0])]
    roi_limit[:,1] = [sum_limit_data[y][1] for y in range(0,sum_limit_data.shape[0])]

def create_roi_name(h5file, sum_name_data):
    try:
        roi_name = h5file.create_dataset("xrfmap/detsum/roi_name", shape=(sum_name_data.shape[0],), dtype = dt)
    except RuntimeError:
        del h5file["xrfmap/detsum/roi_name"]
        roi_name = h5file.create_dataset("xrfmap/detsum/roi_name", shape=(sum_name_data.shape[0],), dtype = dt)
    roi_name[:] = [sum_name_data[y][0] for y in range(0,sum_name_data.shape[0])]

def make_det_datasets(h5file, sum_name_data,sum_limit_data):
    NDETECTORS = get_ndetectors(h5file)
    I0 = h5file['xrfmap/scalers/val']
    I0name = h5file['xrfmap/scalers/name']
    counts = h5file['xrfmap/detsum/counts']
    detcounts = np.zeros(NDETECTORS, dtype=object)
    for detector in range(0, NDETECTORS):
        detcounts[detector] = h5file['xrfmap/det%d/counts' %(detector+1)]

    
    det_name_data = np.empty(shape=(sum_name_data.shape[0]*NDETECTORS,1), dtype=object)
    det_limit_data = np.empty(shape=(sum_name_data.shape[0]*NDETECTORS,2), dtype=int)
    for index in range(0,sum_name_data.shape[0]):
        for detector in range(0,NDETECTORS):
            det_name_data[index*NDETECTORS+detector][0] = sum_name_data[index][0] + ' (mca%d)' %(detector+1)
            det_limit_data[index*NDETECTORS+detector][0] = sum_limit_data[index][0]
            det_limit_data[index*NDETECTORS+detector][1] = sum_limit_data[index][1]

    try:
        det_name = h5file.create_dataset("xrfmap/roimap/det_name", shape=(det_name_data.shape[0]+I0name.size*NDETECTORS,), dtype = dt)
    except RuntimeError:
        del h5file["xrfmap/roimap/det_name"]
        det_name = h5file.create_dataset("xrfmap/roimap/det_name", shape=(det_name_data.shape[0]+I0name.size*NDETECTORS,), dtype = dt)
    
    det_name[0:det_name_data.size] = [det_name_data[y][0] for y in range(0,det_name_data.shape[0])]

    ##put name of scalars into det_name
    for index in range(0,I0name.size):
        for detector in range(1, NDETECTORS+1):
            det_name[det_name_data.size+index*NDETECTORS+detector-1] = I0name[index] + (' (mca %d)' % detector)
    
    try:
        det_raw = h5file.create_dataset("xrfmap/roimap/det_raw", shape = (counts.shape[0],counts.shape[1],det_name.shape[0]+(I0.shape[2]*NDETECTORS)),dtype='i')
    except RuntimeError:
        del h5file["xrfmap/roimap/det_raw"]
        det_raw = h5file.create_dataset("xrfmap/roimap/det_raw", shape = (counts.shape[0],counts.shape[1],det_name.shape[0]+(I0.shape[2]*NDETECTORS)),dtype='i')

    for detector in range(0, NDETECTORS):
        populate_det_raw(det_raw, detcounts[detector], I0, det_limit_data, detector+1, NDETECTORS)
    
    ##put scalars into det_raw
    for index in range(0,I0name.size):
        for detector in range(1, NDETECTORS+1):
            det_raw[:,:,det_name_data.size+index*NDETECTORS+detector-1] = I0[:,:,index]

    h5file['xrfmap/roimap/det_cor'] = h5py.SoftLink('/xrfmap/roimap/det_raw')
            
def make_sum_datasets(h5file,sum_name_data,sum_limit_data):    
    counts = h5file['xrfmap/detsum/counts']
    I0 = h5file['xrfmap/scalers/val']
    I0name = h5file['xrfmap/scalers/name']
    
    try:
        sum_raw = h5file.create_dataset("xrfmap/roimap/sum_raw", shape=(counts.shape[0],counts.shape[1],sum_name_data.shape[0]), dtype='i')
    except RuntimeError:
        del h5file["xrfmap/roimap/sum_raw"]
        sum_raw = h5file.create_dataset("xrfmap/roimap/sum_raw", shape=(counts.shape[0],counts.shape[1],sum_name_data.shape[0]), dtype='i')

    populate_sum_raw(sum_raw, counts, I0, sum_limit_data)

    try:
        sum_name = h5file.create_dataset("xrfmap/roimap/sum_name", shape= (sum_name_data.shape[0],),dtype=varlenstring)
    except RuntimeError:
        del h5file["xrfmap/roimap/sum_name"]
        sum_name = h5file.create_dataset("xrfmap/roimap/sum_name", shape= (sum_name_data.shape[0],),dtype=varlenstring)

    sum_name[:] = [sum_name_data[y] for y in range(0,sum_name_data.shape[0])]
    h5file['xrfmap/roimap/sum_cor'] = h5py.SoftLink('/xrfmap/roimap/sum_raw')

def create_area(h5file):
    counts = h5file['xrfmap/detsum/counts']
    try:
        area_001 = h5file.create_dataset("xrfmap/areas/area_001", shape = (counts.shape[0],counts.shape[1]),dtype=tfenum)
    except RuntimeError:
        del h5file["xrfmap/areas/area_001"]
        area_001 = h5file.create_dataset("xrfmap/areas/area_001", shape = (counts.shape[0],counts.shape[1]),dtype=tfenum)

    try:
        area = h5file.create_group("xrfmap/areas")
        ##holder exists because larch deletes the areas group without it from some reason
        holder = h5file.create_dataset("xrfmap/areas/holder", shape = (0,),dtype='i')
    except ValueError:
        del h5file["xrfmap/areas"]
        area = h5file.create_group("xrfmap/areas")
        holder = h5file.create_dataset("xrfmap/areas/holder", shape = (0,),dtype='i')
        
def create_positions_datasets(h5file):
    counts = h5file['xrfmap/detsum/counts']
    pos = h5file['xrfmap/positions/pos']
    name = h5file['xrfmap/positions/name']
    try:
        larch_name = h5file.create_dataset("xrfmap/positions/name", shape = (4,), dtype= varlenstring)
    except:
        del h5file["xrfmap/positions/name"]
        larch_name = h5file.create_dataset("xrfmap/positions/name", shape = (4,), dtype= varlenstring)

        larch_name[0] = "X"
        larch_name[1] = "Y"
        larch_name[2] = "mca realtime"
        larch_name[3] = "mca livetime"
    try:
        larch_pos = h5file.create_dataset("xrfmap/positions/larch_pos", shape = (counts.shape[0],counts.shape[1],4),dtype='f')
    except RuntimeError:
        del h5file["xrfmap/positions/larch_pos"]
        larch_pos = h5file.create_dataset("xrfmap/positions/larch_pos", shape = (counts.shape[0],counts.shape[1],4),dtype='f')

    ##populate x coords
    larch_pos[:,:,0] = pos[0,:,:]
    ##populate y coords
    larch_pos[:,:,1] = pos[1,:,:]
    ##placeholder for realtime
    larch_pos[:,:,2] = 1
    ##placeholder for livetime
    larch_pos[:,:,3] = 1

    del h5file['xrfmap/positions/pos']
    h5file.create_dataset('xrfmap/positions/pos', shape=larch_pos.shape,data=larch_pos,dtype='f')
    
    del h5file['xrfmap/positions/larch_pos']

def create_detn_datasets(h5file):
    counts = h5file['xrfmap/detsum/counts']
    NDETECTORS = get_ndetectors(h5file)
    for detectornum in range(1,NDETECTORS+1):
        try:
            dtfactor = h5file.create_dataset("xrfmap/det%i/dtfactor"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='f')
        except RuntimeError:
            del h5file["xrfmap/det%i/dtfactor"%detectornum]
            dtfactor = h5file.create_dataset("xrfmap/det%i/dtfactor"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='f')

        try:
            livetime = h5file.create_dataset("xrfmap/det%i/livetime"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='i')
        except RuntimeError:
            del h5file["xrfmap/det%i/livetime"%detectornum]
            livetime = h5file.create_dataset("xrfmap/det%i/livetime"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='i')

        try:
            realtime = h5file.create_dataset("xrfmap/det%i/realtime"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='i')
        except RuntimeError:
            del h5file["xrfmap/det%i/realtime"%detectornum]
            realtime = h5file.create_dataset("xrfmap/det%i/realtime"%detectornum, shape=(counts.shape[0],counts.shape[1]),dtype='i')

        try:
            energy = h5file.create_dataset("xrfmap/det%i/energy"%detectornum, shape=(counts.shape[2],),dtype='f')
        except RuntimeError:
            del h5file["xrfmap/det%i/energy"%detectornum]
            energy = h5file.create_dataset("xrfmap/det%i/energy"%detectornum, shape=(counts.shape[2],),dtype='f')

        energy.attrs.create(name = 'cal_offset', data = 0, shape = None, dtype = 'f')

        energy.attrs.create(name = 'cal_slope', data = 0.01, shape = None, dtype = 'f')

    
        dtfactor[:,:] = 1
        livetime[:,:] = 1
        realtime[:,:] = 1
        for energyValue in range (0, 4096):
            energy[energyValue] = energyValue/100.0

def create_detsum_energy(h5file):
    counts = h5file['xrfmap/detsum/counts']
    try:
        energy = h5file.create_dataset("xrfmap/detsum/energy", shape=(counts.shape[2],),dtype='f')
    except RuntimeError:
        del h5file["xrfmap/detsum/energy"]
        energy = h5file.create_dataset("xrfmap/detsum/energy", shape=(counts.shape[2],),dtype='f')
    
    for energyValue in range (0, 4096):
        energy[energyValue] = energyValue/100.0

    energy.attrs.create(name = 'cal_offset', data = 0, shape = None, dtype = 'f')

    energy.attrs.create(name = 'cal_slope', data = 0.01, shape = None, dtype = 'f')

def main():

    sum_name_data = np.array([["OutputCounts"],
                             ["Si Ka"],
                             ["P Ka"],
                             ["S Ka"],
                             ["Cl Ka"],
                             ["Cd La1"],
                             ["K Ka"],
                             ["Cd Lb1"],
                             ["Ca Ka"],
                             ["Ti Ka"],
                             ["Ce La1"],
                             ["V Ka"],
                             ["Cr Ka"],
                             ["Eu La1"],
                             ["Mn Ka"],
                             ["Fe Ka"],
                             ["Co Ka"],
                             ["Ni Ka"],
                             ["Cu Ka"],
                             ["Zn Ka"],
                             ["As Ka"],
                             ["Se Ka"],
                             ["Br Ka"],
                             ["Rb Ka"],
                             ["Sr Ka"],
                             ["Y Ka"],
                             ["Zr Ka"],
                             ["Mo Ka"]])

    sum_limit_data = np.array([[20,4080],
                           [152,190],
                           [190,214],
                           [212,253],
                           [247,276],
                           [302,323],
                           [311,349],
                           [328,342],
                           [346,388],
                           [430,472],
                           [457,507],
                           [468,519],
                           [520,565],
                           [551,614],
                           [567,609],
                           [610,672],
                           [675,708],
                           [728,773],
                           [778,829],
                           [830,891],
                           [1021,1089],
                           [1076,1159],
                           [1156,1219],
                           [1307,1364],
                           [1383,1444],
                           [1475,1507],
                           [1527,1628],
                           [1684,1787]])

    
    files = sys.argv[1:]
    if(files==[]):
        print("Must provide file path argument")
        print('Use -help for more info')
        exit(2)

    if(files[0] == '-help'):
        print('The NSLSII to Larch converter scripts expects a filepath or filepaths from the command line aruments. You must give the filepath relative to the current working directory. To call multiple files, simply provide multiple paths to each file. If you want to convert all files in a directory, use *.h5 as your filename.')
        exit(1)

    ##Check if all filepaths are valid
    for filename in files:
        try:
            open(filename, 'r').close()
        except:
            print(filename, ' is not a valid file path')
            print('Use -help for more info')
            exit(2)

    for filename in files:
        print("Converting ", filename,'...')
        h5file = h5py.File(filename, 'r+')
        NDETECTORS = get_ndetectors(h5file)
        I0name = h5file['xrfmap/scalers/name']
        
        create_roi_limit(h5file,sum_limit_data)
        create_roi_name(h5file,sum_name_data)

        sum_name_data = np.insert(sum_name_data, 0, I0name[()])
        I0placeholder = np.ndarray(shape=(I0name.size,2))
        I0placeholder.fill(-1)
        sum_limit_data = np.insert(sum_limit_data, 0, I0placeholder, 0)

        print("Step 1 of 2")
        make_sum_datasets(h5file,sum_name_data,sum_limit_data)
        print("Step 2 of 2")
        make_det_datasets(h5file,sum_name_data,sum_limit_data)
        print("Finishing up...")
        create_area(h5file)
        create_positions_datasets(h5file)
        create_detn_datasets(h5file)
        create_detsum_energy(h5file)

        add_larch_datasets(h5file)
        add_larch_attributes(h5file) 
        add_from_template(h5file)
        h5file.close()

if __name__ == "__main__":
    main()
