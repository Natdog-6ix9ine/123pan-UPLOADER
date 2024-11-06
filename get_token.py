import os
import json
import time
import hashlib
import requests
import subprocess
import urllib.parse
import math
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone


class TokenManager:
    def __init__(self, config_path='config.json'):
        self.config_path = config_path
        self.base_url = 'https://open-api.123pan.com'
        self.config = self.load_config()

    def load_config(self) -> dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"读取配置文件失败: {str(e)}")

    def save_config(self):
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, ensure_ascii=False, indent=2)

    def get_access_token(self) -> str:
        url = f"{self.base_url}/api/v1/access_token"
        headers = {
            'Platform': 'open_platform',
            'Content-Type': 'application/json'
        }
        payload = {
            "clientID": self.config['client_id'],
            "clientSecret": self.config['client_secret']
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        if data['code'] == 0:
            self.config['access_token'] = data['data']['accessToken']
            self.config['expired_at'] = data['data']['expiredAt']
            self.config['last_updated'] = datetime.now(timezone.utc).isoformat()
            self.save_config()
            print("Access token 获取成功并已保存到配置文件")
            return data['data']['accessToken']
        else:
            raise Exception(f"获取access token失败: {data['message']}")

class Pan123FileManager:
    def __init__(self, config_path='config.json'):
        self.config_path = config_path
        self.base_url = 'https://open-api.123pan.com'
        self.config = self.load_config()
        self.headers = {
            'Platform': 'open_platform',
            'Authorization': f"Bearer {self.config['access_token']}",
            'Content-Type': 'application/json'
        }

    def load_config(self) -> dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"读取配置文件失败: {str(e)}")

    def save_config(self):
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, ensure_ascii=False, indent=2)

    def list_files(self, parent_id: int = 0, limit: int = 100, search_data: str = None, search_mode: int = 0) -> List[Dict]:
        url = f"{self.base_url}/api/v2/file/list"
        params = {
            'parentFileId': parent_id,
            'limit': limit
        }
        if search_data:
            params['searchData'] = search_data
            params['searchMode'] = search_mode

        files = []
        while True:
            response = requests.get(url, headers=self.headers, params=params)
            data = response.json()

            if data['code'] != 0:
                raise Exception(f"获取文件列表失败: {data['message']}")

            files.extend(data['data']['fileList'])

            if data['data']['lastFileId'] == -1:  # 最后一页
                break
            params['lastFileId'] = data['data']['lastFileId']

        return files

    def calculate_file_md5(self, file_path: str) -> str:
        """计算文件MD5"""
        md5_hash = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def calculate_chunk_md5(self, chunk: bytes) -> str:
        """计算分片MD5"""
        return hashlib.md5(chunk).hexdigest()

    def create_directory(self, name: str, parent_id: int = 0) -> int:
        """创建目录"""
        url = f"{self.base_url}/upload/v1/file/mkdir"
        payload = {
            "name": name,
            "parentID": parent_id
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return data['data']['dirID']
        raise Exception(f"创建目录失败: {data['message']}")

    def create_file(self, filename: str, file_path: str, parent_id: int = 0) -> Tuple[str, int, bool, int]:
        """创建文件，返回预上传ID、文件ID、是否秒传、分片大小"""
        file_size = os.path.getsize(file_path)
        file_md5 = self.calculate_file_md5(file_path)
        
        url = f"{self.base_url}/upload/v1/file/create"
        payload = {
            "parentFileID": parent_id,
            "filename": filename,
            "etag": file_md5,
            "size": file_size
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return (
                data['data'].get('preuploadID', ''),
                data['data'].get('fileID', 0),
                data['data'].get('reuse', False),
                data['data'].get('sliceSize', 0)
            )
        raise Exception(f"创建文件失败: {data['message']}")

    def get_upload_url(self, preupload_id: str, slice_no: int) -> str:
        """获取上传地址"""
        url = f"{self.base_url}/upload/v1/file/get_upload_url"
        payload = {
            "preuploadID": preupload_id,
            "sliceNo": slice_no
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return data['data']['presignedURL']
        raise Exception(f"获取上传地址失败: {data['message']}")

    def list_uploaded_parts(self, preupload_id: str) -> List[Dict]:
        """列举已上传分片"""
        url = f"{self.base_url}/upload/v1/file/list_upload_parts"
        payload = {"preuploadID": preupload_id}
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return data['data']['parts']
        raise Exception(f"列举分片失败: {data['message']}")

    def complete_upload(self, preupload_id: str) -> Tuple[int, bool, bool]:
        """完成上传"""
        url = f"{self.base_url}/upload/v1/file/upload_complete"
        payload = {"preuploadID": preupload_id}
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return (
                data['data'].get('fileID', 0),
                data['data']['async'],
                data['data']['completed']
            )
        raise Exception(f"完成上传失败: {data['message']}")

    def check_async_result(self, preupload_id: str) -> Tuple[bool, int]:
        """检查异步上传结果"""
        url = f"{self.base_url}/upload/v1/file/upload_async_result"
        payload = {"preuploadID": preupload_id}
        
        response = requests.post(url, headers=self.headers, json=payload)
        data = response.json()
        
        if data['code'] == 0:
            return data['data']['completed'], data['data']['fileID']
        raise Exception(f"检查异步结果失败: {data['message']}")

    def upload_file(self, file_path: str, parent_id: int = 0) -> int:
        """上传文件的主函数"""
        original_filename = os.path.basename(file_path)
        filename = original_filename
        print(f"开始上传文件: {filename}")

        # 检查目标文件夹是否存在同名文件
        existing_file = self.check_file_exists(filename, parent_id)
        if existing_file:
            upload_copy = input(f"目标文件夹已存在同名文件 '{filename}'。是否上传副本？(y/n): ").lower().strip()
            if upload_copy != 'y':
                print("上传已取消。")
                sys.exit(0)
            else:
                # 创建一个新的文件名
                name, ext = os.path.splitext(filename)
                counter = 1
                while self.check_file_exists(filename, parent_id):
                    filename = f"{name}_copy{counter}{ext}"
                    counter += 1
                print(f"文件将以新名称上传: {filename}")

        # 1. 创建文件
        preupload_id, file_id, is_reuse, slice_size = self.create_file(filename, file_path, parent_id)
        
        if is_reuse:
            print("文件秒传成功！")
            return file_id

        print(f"文件需要分片上传，分片大小: {slice_size} bytes")
        
        # 2. 分片上传
        file_size = os.path.getsize(file_path)
        total_chunks = math.ceil(file_size / slice_size)
        uploaded_chunks = []

        with open(file_path, 'rb') as f:
            for chunk_index in range(total_chunks):
                chunk = f.read(slice_size)
                slice_no = chunk_index + 1
                
                # 获取上传URL
                upload_url = self.get_upload_url(preupload_id, slice_no)
                
                # 上传分片
                print(f"上传分片 {slice_no}/{total_chunks}")
                headers = {'Content-Type': 'application/octet-stream'}
                response = requests.put(upload_url, headers=headers, data=chunk)
                
                if response.status_code != 200:
                    raise Exception(f"分片 {slice_no} 上传失败: {response.text}")
                
                uploaded_chunks.append({
                    'partNumber': slice_no,
                    'etag': self.calculate_chunk_md5(chunk)
                })

        # 3. 验证分片（如果文件大于分片大小）
        if file_size > slice_size:
            print("验证已上传分片...")
            server_parts = self.list_uploaded_parts(preupload_id)
            
            # 验证每个分片
            for local_part in uploaded_chunks:
                server_part = next(
                    (p for p in server_parts if p['partNumber'] == local_part['partNumber']),
                    None
                )
                if not server_part or server_part['etag'] != local_part['etag']:
                    raise Exception(f"分片 {local_part['partNumber']} 验证失败")

        # 4. 完成上传
        print("完成上传...")
        file_id, is_async, is_completed = self.complete_upload(preupload_id)

        # 5. 如果需要，等待异步结果
        if is_async:
            print("等待服务器处理...")
            while True:
                completed, final_file_id = self.check_async_result(preupload_id)
                if completed:
                    file_id = final_file_id
                    break
                time.sleep(1)

        print(f"文件上传成功！文件ID: {file_id}")
        return file_id

    def list_folders(self, parent_id: int = 0) -> List[Dict]:
        """获取指定目录下的所有文件夹"""
        url = f"{self.base_url}/api/v2/file/list"
        params = {
            'parentFileId': parent_id,
            'limit': 100,  # 假设每页最多100个项目
        }
        folders = []

        while True:
            response = requests.get(url, headers=self.headers, params=params)
            data = response.json()

            if data['code'] != 0:
                raise Exception(f"获取文件夹列表失败: {data['message']}")

            folders.extend([f for f in data['data']['fileList'] if f['type'] == 1])  # type 1 表示文件夹

            if data['data']['lastFileId'] == -1:  # -1 表示没有更多页面
                break
            params['lastFileId'] = data['data']['lastFileId']

        return folders

    def check_file_exists(self, filename: str, parent_id: int) -> Optional[Dict]:
        """检查指定目录下是否存在同名文件"""
        url = f"{self.base_url}/api/v2/file/list"
        params = {
            'parentFileId': parent_id,
            'limit': 100,
            'searchData': filename,
            'searchMode': 1  # 精确搜索
        }

        response = requests.get(url, headers=self.headers, params=params)
        data = response.json()

        if data['code'] != 0:
            raise Exception(f"检查文件是否存在失败: {data['message']}")

        for file in data['data']['fileList']:
            if file['filename'] == filename:
                return file

        return None

    def get_download_url(self, file_id: int) -> str:
        """获取文件的下载链接"""
        url = f"{self.base_url}/api/v2/file/download_address"
        params = {'fileID': file_id}
        response = requests.get(url, headers=self.headers, params=params)
        data = response.json()

        if data['code'] != 0:
            raise Exception(f"获取下载链接失败: {data['message']}")

        return data['data']['downloadAddress']

    def download_file(self, download_url: str, save_path: str):
        """使用wget下载文件"""
        # 解析URL中的文件名
        parsed_url = urllib.parse.urlparse(download_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        original_filename = query_params.get('filename', [''])[0]
        original_filename = urllib.parse.unquote(original_filename)

        # 询问是否保持原文件名
        keep_original_name = input(f"是否保持原文件名 '{original_filename}'？(y/n): ").strip().lower()
        if keep_original_name == 'y':
            filename = original_filename
        else:
            new_filename = input("请输入新的文件名（保持原后缀）: ").strip()
            original_ext = os.path.splitext(original_filename)[1]
            filename = new_filename if new_filename.endswith(original_ext) else new_filename + original_ext

        full_save_path = os.path.join(save_path, filename)

        # 使用wget下载文件
        wget_command = ['wget', '-O', full_save_path, download_url]
        try:
            subprocess.run(wget_command, check=True)
            print(f"文件已成功下载到: {full_save_path}")
        except subprocess.CalledProcessError as e:
            print(f"下载失败: {e}")

def select_file_or_folder(file_manager: Pan123FileManager, parent_id: int = 0) -> Tuple[int, bool]:
    while True:
        print(f"\n当前文件夹ID: {parent_id}")
        items = file_manager.list_files(parent_id)
        for item in items:
            item_type = "文件夹" if item['type'] == 1 else "文件"
            print(f"ID: {item['fileId']}, 名称: {item['filename']}, 类型: {item_type}")
        
        choice = input("请选择操作：\n1. 选择当前项\n2. 进入文件夹\n3. 返回上级\n请输入选项(1/2/3): ").strip()
        
        if choice == '1':
            item_id = int(input("请输入要选择的项目ID: ").strip())
            selected_item = next((item for item in items if item['fileId'] == item_id), None)
            if selected_item:
                return item_id, selected_item['type'] == 1
            else:
                print("无效的ID，请重新选择。")
        elif choice == '2':
            folder_id = int(input("请输入要进入的文件夹ID: ").strip())
            parent_id = folder_id
        elif choice == '3':
            if parent_id == 0:
                print("已经在根目录，无法返回上级。")
            else:
                parent_id = 0  # 简化处理，直接返回根目录
        else:
            print("无效的选择，请重新输入。")

def main():
    config_path = 'config.json'
    token_manager = TokenManager(config_path)
    file_manager = Pan123FileManager(config_path)

    while True:
        action = input("请选择操作：\n1. 获取access token\n2. 下载文件\n3. 上传文件\n4. 退出\n请输入选项(1/2/3/4): ").strip()
        
        if action == '1':
            try:
                token_manager.get_access_token()
                file_manager = Pan123FileManager(config_path)  # 刷新file_manager以使用新的token
            except Exception as e:
                print(f"获取access token失败: {str(e)}")
        
        elif action == '2':
            try:
                use_config = input("是否使用config.json中的下载设置？(y/n): ").strip().lower()
                if use_config == 'y':
                    download_url = file_manager.config.get('download_url')
                    save_path = file_manager.config.get('download_path')
                    if not download_url or not save_path:
                        raise ValueError("配置文件中缺少下载URL或保存路径")
                else:
                    print("请选择要下载的文件：")
                    file_id, is_folder = select_file_or_folder(file_manager)
                    if is_folder:
                        print("您选择了一个文件夹，请选择一个文件进行下载。")
                        continue
                    download_url = file_manager.get_download_url(file_id)
                    save_path = input("请输入保存路径: ").strip()

                file_manager.download_file(download_url, save_path)
            except Exception as e:
                print(f"下载文件失败: {str(e)}")
        
        elif action == '3':
            while True:
                use_config = input("是否使用配置文件中的上传文件和目标网盘ID? (y/n): ").lower().strip()
                
                if use_config == 'y':
                    file_path = file_manager.config.get('upload_file_path')
                    parent_id = file_manager.config.get('parent_folder_id', 0)
                    
                    if not file_path:
                        print("错误：配置文件中未指定上传文件路径")
                        continue
                    
                    try:
                        file_id = file_manager.upload_file(file_path, parent_id)
                        print(f"文件上传完成，文件ID: {file_id}")
                    except Exception as e:
                        print(f"上传失败: {str(e)}")
                    
                    break
                elif use_config == 'n':
                    print("选择目标文件夹：")
                    new_parent_id = select_file_or_folder(file_manager)[0]
                    file_manager.config['parent_folder_id'] = new_parent_id
                    file_manager.save_config()
                    print(f"已更新配置文件中的目标文件夹ID: {new_parent_id}")
                    
                    file_path = input("请输入要上传的文件路径: ").strip()
                    file_manager.config['upload_file_path'] = file_path
                    file_manager.save_config()
                    
                    try:
                        file_id = file_manager.upload_file(file_path, new_parent_id)
                        print(f"文件上传完成，文件ID: {file_id}")
                    except Exception as e:
                        print(f"上传失败: {str(e)}")
                    
                    break
                else:
                    print("无效的输入，请输入 y 或 n")
        
        elif action == '4':
            print("程序退出")
            break
        
        else:
            print("无效的选择，请重新输入。")

if __name__ == "__main__":
    main()