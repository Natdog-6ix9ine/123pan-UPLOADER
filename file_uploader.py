import os
import json
import time
import hashlib
import requests
from typing import Dict, List, Tuple, Optional
import math

class Pan123Uploader:
    def __init__(self, config_path='config.json'):
        self.base_url = 'https://open-api.123pan.com'
        self.config = self.load_config(config_path)
        self.headers = {
            'Platform': 'open_platform',
            'Authorization': f"Bearer {self.config['access_token']}",
            'Content-Type': 'application/json'
        }

    def load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"读取配置文件失败: {str(e)}")

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
        filename = os.path.basename(file_path)
        print(f"开始上传文件: {filename}")

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
                response = requests.put(upload_url, data=chunk)
                
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

def main():
    uploader = Pan123Uploader()
    
    # 获取用户输入
    file_path = input("请输入要上传的文件路径: ").strip()
    parent_id_input = input("请输入父目录ID (默认为0, 即根目录): ").strip()
    parent_id = int(parent_id_input) if parent_id_input else 0

    try:
        file_id = uploader.upload_file(file_path, parent_id)
        print(f"文件上传完成，文件ID: {file_id}")
    except Exception as e:
        print(f"上传失败: {str(e)}")

if __name__ == "__main__":
    main()