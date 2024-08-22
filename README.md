# Python RAC



## Общая информация

Реализация основана на статьях: https://infostart.ru/1c/articles/1503913/ и https://infostart.ru/1c/tools/1519541/

Формат данных описан в https://github.com/v8platform/protos

## Примеры использования

* Список кластеров:

``rac_client.py --ras-host=localhost --ras-port=1545 cluster list``

* Информация о кластере:

``rac_client.py --ras-host=localhost --ras-port=1545 cluster info --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92``

* Установка времени перезапуска рабочих процессов:

``rac_client.py --ras-host=localhost --ras-port=1545 cluster update --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --agent-user=clusteradmin --agent-pwd=clusterpassword --lifetime-limit=86400 --expiration-timeout=180``

* Список ИБ:

``rac_client.py --ras-host=localhost --ras-port=1545 infobase --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword summary list``

* Информация о ИБ:

``rac_client.py --ras-host=localhost --ras-port=1545 infobase --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword info --infobase=779e935b-7cfc-4b3e-a36e-fff3a8dd693f --infobase-user=Admin --infobase-pwd=clusterpassword``

* Установка блокировки сеансов:

``rac_client.py --ras-host=localhost --ras-port=1545 infobase --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword update --infobase=779e935b-7cfc-4b3e-a36e-fff3a8dd693f --infobase-user=Admin --infobase-pwd=clusterpassword --denied-message="" --sessions-deny=on --scheduled-jobs-deny=on --permission-code="95876123" --denied-from="2024-01-01 23:00:00" --denied-to=""``

* Список сеансов:

``rac_client.py --ras-host=localhost --ras-port=1545 session --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword list``

* Завершение определенного сеанса:

``rac_client.py --ras-host=localhost --ras-port=1545 session --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword terminate --session=1da9758f-3c10-4064-9501-bc848863bbc3``

* Завершение всех сеансов ИБ:

``rac_client.py --ras-host=localhost --ras-port=1545 session --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword terminate --infobase=779e935b-7cfc-4b3e-a36e-fff3a8dd693f``

* Завершение всех сеансов кластера:

``rac_client.py --ras-host=localhost --ras-port=1545 session --cluster=0cec4877-38b8-4fa1-8ee3-3c6623ba7c92 --cluster-user=clusteradmin --cluster-pwd=clusterpassword terminate``

