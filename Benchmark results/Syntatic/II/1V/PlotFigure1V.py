
# coding: utf-8

# In[1]:

import matplotlib.pyplot as plt
import pandas as pd

plt.xlabel('Time (s)')
plt.ylabel('Number of Sliding Window')
plt.title('The number of sliding window executed')

plt.xlim(20,700)
plt.ylim(0, 0.7)


time2 = ['14', '34', '54', '74', '94', '114', '134', '155', '175', '195', '215', '235', '255', '275', '296', '316', '336', '356', '376', '396', '416', '437', '457', '477', '497', '517', '537', '557', '577', '598', '618', '638', '658', '678', '698']
execution2 = ['0', '878.000', '617.192', '627.592', '582.155', '598.467', '623.242', '571.223', '564.524', '558.016', '534.238', '527.928', '517.154', '505.651', '515.130', '512.763', '503.094', '508.665', '513.973', '509.868', '514.609', '513.092', '525.623', '532.633', '543.233', '534.064', '524.438', '526.925', '531.022', '522.641', '520.839', '521.744', '523.865', '518.586', '520.569']
processlatency2 = ['0.000', '0.010', '0.019', '0.016', '0.015', '0.012', '0.012', '0.012', '0.013', '0.011', '0.011', '0.012', '0.011', '0.011', '0.011', '0.011', '0.010', '0.010', '0.010', '0.011', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.010', '0.009', '0.010', '0.010']
executionlatency2 = ['0.000', '88.343', '495.808', '501.347', '585.767', '552.635', '571.951', '539.961', '545.943', '566.986', '558.054', '555.258', '543.538', '540.410', '536.943', '524.623', '521.096', '517.678', '509.926', '500.203', '500.535', '505.638', '502.163', '499.282', '504.044', '504.940', '503.204', '505.282', '513.157', '507.868', '500.825', '508.430', '509.720', '505.098', '504.339']
swnumber2 = [26.41509433962264, 64.15094339622641, 101.88679245283019, 139.62264150943398, 177.35849056603774, 215.0943396226415, 252.83018867924528, 292.45283018867923, 330.188679245283, 367.92452830188677, 405.66037735849056, 443.39622641509436, 481.1320754716981, 518.8679245283018, 558.4905660377359, 596.2264150943396, 633.9622641509434, 671.6981132075472, 709.433962264151, 747.1698113207547, 784.9056603773585, 824.5283018867924, 862.2641509433962, 900.0, 937.7358490566038, 975.4716981132076, 1013.2075471698113, 1050.9433962264152, 1088.6792452830189, 1128.301886792453, 1166.0377358490566, 1203.7735849056603, 1241.5094339622642, 1279.245283018868, 1316.9811320754718]
transfer2 = [0.0, 0.22447058823529412, 0.2826666666666666, 0.3781621621621621, 0.43302127659574463, 0.4463157894736842, 0.4556417910447761, 0.49238709677419357, 0.4942628571428571, 0.5088, 0.5088, 0.5196255319148936, 0.5287529411764705, 0.5273018181818182, 0.5242702702702703, 0.5313417721518988, 0.53, 0.5288089887640449, 0.5345106382978723, 0.5332121212121212, 0.5442692307692307, 0.535578947368421, 0.5455404814004376, 0.5439999999999999, 0.5425835010060361, 0.5511179883945841, 0.5495418994413408, 0.5480789946140036, 0.5511265164644714, 0.5530434782608695, 0.5516116504854368, 0.5542570532915361, 0.5567416413373859, 0.5553274336283186, 0.5576389684813753]




time4 = ['16', '37', '57', '77', '97', '117', '137', '158', '178', '198', '218', '238', '258', '278', '298', '319', '339', '359', '379', '399', '419', '439', '460', '480', '500', '520', '540', '560', '580', '601', '621', '641', '661', '681', '701']
execution4 = ['0', '222.333', '252.125', '235.429', '237.775', '243.836', '236.256', '237.408', '227.534', '235.680', '234.206', '234.857', '240.843', '246.249', '240.587', '239.137', '243.646', '247.401', '248.351', '241.731', '243.972', '249.203', '250.698', '248.131', '246.443', '244.993', '244.327', '243.961', '245.283', '245.718', '245.473', '246.606', '249.531', '250.189', '249.875']
processlatency4 = ['0.000', '0.056', '0.038', '0.032', '0.028', '0.023', '0.021', '0.020', '0.019', '0.018', '0.017', '0.017', '0.017', '0.018', '0.017', '0.017', '0.016', '0.016', '0.016', '0.016', '0.015', '0.015', '0.015', '0.015', '0.015', '0.015', '0.015', '0.014', '0.015', '0.015', '0.015', '0.015', '0.014', '0.014', '0.014']
executionlatency4 = ['0.000', '241.895', '245.963', '236.795', '193.201', '197.678', '189.563', '200.923', '215.226', '227.531', '232.733', '230.859', '226.592', '228.701', '230.359', '239.131', '235.974', '241.224', '240.492', '239.049', '238.382', '239.222', '237.466', '238.191', '238.418', '238.828', '240.274', '239.801', '238.166', '237.650', '236.928', '234.715', '235.291', '233.982', '238.869']
swnumber4 = [66.66666666666667, 154.16666666666666, 237.5, 320.8333333333333, 404.1666666666667, 487.5, 570.8333333333334, 658.3333333333334, 741.6666666666666, 825.0, 908.3333333333334, 991.6666666666666, 1075.0, 1158.3333333333333, 1241.6666666666667, 1329.1666666666667, 1412.5, 1495.8333333333333, 1579.1666666666667, 1662.5, 1745.8333333333333, 1829.1666666666667, 1916.6666666666667, 2000.0, 2083.3333333333335, 2166.6666666666665, 2250.0, 2333.3333333333335, 2416.6666666666665, 2504.1666666666665, 2587.5, 2670.8333333333335, 2754.1666666666665, 2837.5, 2920.8333333333335]
transfer4 = [0.0, 0.12454054054054055, 0.18189473684210525, 0.1944935064935065, 0.21377319587628862, 0.22153846153846155, 0.22283211678832113, 0.22967088607594935, 0.2329887640449438, 0.23272727272727273, 0.23779816513761468, 0.24201680672268908, 0.24111627906976743, 0.24448920863309354, 0.2454765100671141, 0.2437617554858934, 0.2446725663716814, 0.24869080779944291, 0.24772559366754615, 0.24974436090225563, 0.24882100238663485, 0.24929384965831433, 0.24918260869565215, 0.252, 0.24883199999999997, 0.25033846153846157, 0.25066666666666665, 0.2530285714285714, 0.2512551724137931, 0.25205990016638935, 0.2513623188405797, 0.25160686427457096, 0.25270801815431165, 0.2528986784140969, 0.2522567760342368]




time6 = ['22', '42', '62', '82', '102', '122', '142', '162', '183', '203', '223', '243', '263', '283', '303', '323', '344', '364', '384', '404', '424', '444', '464', '484', '504', '525', '545', '565', '585', '605', '625', '645', '665', '685']
execution6 = ['0', '167.450', '142.644', '152.909', '153.798', '159.673', '166.302', '171.804', '169.663', '172.057', '171.674', '168.858', '164.262', '168.472', '166.926', '170.402', '170.997', '175.650', '177.941', '173.837', '172.606', '175.495', '173.928', '172.021', '171.978', '172.036', '173.822', '173.048', '172.233', '171.408', '170.901', '169.837', '168.297', '167.899']
processlatency6 = ['0.000', '0.027', '0.029', '0.028', '0.027', '0.027', '0.027', '0.026', '0.024', '0.023', '0.023', '0.023', '0.022', '0.022', '0.022', '0.022', '0.022', '0.022', '0.022', '0.022', '0.021', '0.021', '0.021', '0.021', '0.021', '0.021', '0.021', '0.021', '0.021', '0.021', '0.020', '0.020', '0.020', '0.020']
executionlatency6 = ['0.000', '75.987', '103.973', '109.423', '138.004', '129.919', '133.034', '141.517', '146.385', '139.731', '140.015', '136.497', '134.845', '138.899', '139.918', '143.944', '145.160', '146.695', '147.826', '146.201', '147.432', '145.660', '144.840', '143.210', '144.132', '143.714', '146.115', '144.360', '143.498', '145.070', '145.599', '144.776', '143.699', '145.274']
swnumber6 = [137.5, 262.5, 387.5, 512.5, 637.5, 762.5, 887.5, 1012.5, 1143.75, 1268.75, 1393.75, 1518.75, 1643.75, 1768.75, 1893.75, 2018.75, 2150.0, 2275.0, 2400.0, 2525.0, 2650.0, 2775.0, 2900.0, 3025.0, 3150.0, 3281.25, 3406.25, 3531.25, 3656.25, 3781.25, 3906.25, 4031.25, 4156.25, 4281.25]
transfer6 = [0.0, 0.07314285714285713, 0.1032258064516129, 0.11863414634146342, 0.13301960784313724, 0.14059016393442622, 0.1424225352112676, 0.1485432098765432, 0.1524808743169399, 0.1525911330049261, 0.15497757847533633, 0.1590781893004115, 0.15768821292775664, 0.15920848056537104, 0.16221782178217822, 0.16089164086687308, 0.16148837209302325, 0.16316483516483515, 0.16333333333333333, 0.16411881188118813, 0.1648301886792453, 0.1649009009009009, 0.16551724137931034, 0.16608264462809919, 0.1655873015873016, 0.16627809523809525, 0.16675229357798166, 0.16673982300884957, 0.16760341880341884, 0.16798677685950414, 0.1675264, 0.16788837209302326, 0.1686135338345865, 0.16854890510948906]



time8 = ['11', '32', '52', '72', '92', '112', '132', '152', '173', '193', '213', '233', '253', '273', '293', '313', '333', '354', '374', '394', '414', '434', '454', '474', '494', '515', '535', '555', '575', '595', '615', '635', '655', '675', '696']
execution8 = ['0', '395.800', '191.074', '165.467', '157.164', '142.083', '150.899', '146.493', '143.186', '135.264', '133.789', '135.615', '140.336', '136.848', '133.376', '135.455', '134.785', '137.212', '136.037', '136.979', '137.077', '140.287', '139.311', '140.208', '140.242', '137.927', '138.727', '137.548', '137.830', '136.979', '135.959', '134.537', '133.972', '134.127', '133.241']
processlatency8 = ['0.000', '0.076', '0.068', '0.051', '0.048', '0.044', '0.042', '0.039', '0.038', '0.038', '0.037', '0.036', '0.036', '0.036', '0.035', '0.037', '0.037', '0.036', '0.036', '0.036', '0.036', '0.036', '0.035', '0.035', '0.034', '0.034', '0.034', '0.033', '0.033', '0.033', '0.033', '0.033', '0.033', '0.032', '0.032']
executionlatency8 = ['0.000', '87.195', '116.214', '105.905', '109.684', '105.508', '112.131', '120.412', '124.695', '120.401', '119.711', '116.870', '118.281', '115.973', '115.917', '113.015', '111.424', '112.136', '109.975', '112.516', '111.559', '113.636', '113.819', '114.187', '113.878', '114.402', '115.413', '116.189', '116.772', '116.805', '117.039', '117.288', '116.445', '116.228', '116.404']
swnumber8 = [73.33333333333333, 213.33333333333334, 346.6666666666667, 480.0, 613.3333333333334, 746.6666666666666, 880.0, 1013.3333333333334, 1153.3333333333333, 1286.6666666666667, 1420.0, 1553.3333333333333, 1686.6666666666667, 1820.0, 1953.3333333333333, 2086.6666666666665, 2220.0, 2360.0, 2493.3333333333335, 2626.6666666666665, 2760.0, 2893.3333333333335, 3026.6666666666665, 3160.0, 3293.3333333333335, 3433.3333333333335, 3566.6666666666665, 3700.0, 3833.3333333333335, 3966.6666666666665, 4100.0, 4233.333333333333, 4366.666666666667, 4500.0, 4640.0]
transfer8 = [0.0, 0.056249999999999994, 0.08307692307692306, 0.1075, 0.12326086956521737, 0.12696428571428572, 0.13363636363636364, 0.13855263157894737, 0.13838150289017342, 0.1417616580310881, 0.14450704225352112, 0.14446351931330473, 0.1458498023715415, 0.14835164835164835, 0.14744027303754267, 0.1489456869009585, 0.15027027027027026, 0.15, 0.1506417112299465, 0.1516751269035533, 0.15173913043478263, 0.15262672811059905, 0.1526431718061674, 0.15227848101265823, 0.15303643724696356, 0.15343689320388348, 0.15274766355140187, 0.15405405405405406, 0.1543304347826087, 0.1542857142857143, 0.15453658536585366, 0.15505511811023623, 0.1547175572519084, 0.1552, 0.1554310344827586]




time10 = ['11', '31', '51', '71', '91', '111', '132', '152', '172', '192', '212', '232', '253', '273', '293', '313', '333', '353', '373', '393', '414', '434', '454', '474', '494', '514', '534', '554', '575', '595', '615', '635', '655', '675', '695']
execution10 = ['0', '225.600', '186.630', '145.091', '136.049', '127.574', '123.955', '121.615', '120.808', '119.157', '120.084', '121.811', '121.368', '122.495', '121.276', '119.305', '118.459', '117.824', '117.225', '117.660', '116.364', '114.931', '114.469', '113.220', '112.596', '114.213', '114.437', '114.050', '114.141', '113.824', '113.888', '114.589', '114.189', '114.393', '114.478']
processlatency10 = ['0.000', '0.286', '0.116', '0.085', '0.080', '0.069', '0.064', '0.061', '0.058', '0.054', '0.053', '0.053', '0.052', '0.050', '0.050', '0.051', '0.051', '0.051', '0.050', '0.050', '0.050', '0.050', '0.050', '0.050', '0.049', '0.049', '0.049', '0.049', '0.048', '0.048', '0.048', '0.048', '0.048', '0.048', '0.047']
executionlatency10 = ['0.000', '34.216', '83.030', '85.016', '76.200', '79.937', '80.918', '79.973', '82.083', '82.501', '79.595', '77.907', '77.863', '79.270', '82.131', '81.458', '83.991', '85.865', '86.061', '86.558', '86.076', '88.045', '88.799', '87.248', '88.053', '87.711', '88.020', '88.925', '88.734', '88.680', '88.964', '89.286', '88.685', '88.678', '88.722']
swnumber10 = [91.66666666666667, 258.3333333333333, 425.0, 591.6666666666666, 758.3333333333334, 925.0, 1100.0, 1266.6666666666667, 1433.3333333333333, 1600.0, 1766.6666666666667, 1933.3333333333333, 2108.3333333333335, 2275.0, 2441.6666666666665, 2608.3333333333335, 2775.0, 2941.6666666666665, 3108.3333333333335, 3275.0, 3450.0, 3616.6666666666665, 3783.3333333333335, 3950.0, 4116.666666666667, 4283.333333333333, 4450.0, 4616.666666666667, 4791.666666666667, 4958.333333333333, 5125.0, 5291.666666666667, 5458.333333333333, 5625.0, 5791.666666666667]
transfer10 = [0.0, 0.03716129032258065, 0.0700235294117647, 0.08923943661971832, 0.09874285714285713, 0.10170810810810811, 0.10734545454545455, 0.10989473684210527, 0.10984186046511628, 0.11220000000000001, 0.1146566037735849, 0.11470344827586207, 0.11611067193675889, 0.11730989010989011, 0.1167726962457338, 0.11888051118210861, 0.12004324324324324, 0.11944249291784703, 0.12014155495978551, 0.12106259541984733, 0.12048695652173913, 0.12130506912442397, 0.12179735682819383, 0.12151898734177215, 0.12196275303643724, 0.12259610894941635, 0.12253483146067415, 0.12289386281588448, 0.1230135652173913, 0.1223636974789916, 0.12306731707317074, 0.12336377952755906, 0.12346625954198473, 0.12356266666666665, 0.12398503597122303]



plot1 = plt.plot(time2, transfer2,'r*-', label='G=2')
plot2 = plt.plot(time4, transfer4, 'bs-', label='G=4')
plot3 = plt.plot(time6, transfer6, 'g^-', label='G=6')
plot4 = plt.plot(time8, transfer8, 'c+-', label='G=8')
plot5 = plt.plot(time10, transfer10, 'mo-', label='G=10')


plt.legend(loc='upper center', ncol=3, shadow=True,)

plt.savefig('./Fig/DataTransfer.png')

