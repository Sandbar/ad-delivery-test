import os
import sys
import ext_service.mongo as mongo

pro_home = __file__[:__file__.rfind("/")]
pid_filename = os.path.join(pro_home, 'pid.log')
os.remove(pid_filename)
os.mknod(pid_filename)
# 启动反馈监听程序
os.popen(
    'nohup ' + sys.executable + ' ' + pro_home + '/listener.py >> ' + pro_home + '/out/listen.out' +
    ' 2>&1 </dev/null &')
#启动投放程序
delt_names = mongo.get_deliverys()
for name in delt_names:
    os.popen('nohup ' + sys.executable + ' ' + pro_home + '/delivery.py ' + name + '>> ' +
             pro_home + '/out/' + name + '.out 2>&1 </dev/null &')
