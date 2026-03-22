# 1. 基础镜像：使用官方 Python 3.10 Slim 版本 (体积小，仅含运行时核心)
FROM python:3.10-slim

# 2. 设置容器内的工作目录
WORKDIR /app

# 3. 设置环境变量：PYTHONUNBUFFERED=1 
# 作用：让 Python 的 print 输出不经过缓存直接打印到 Docker 日志中
# (如果不加这句，你用 docker logs 可能看不到实时输出)
ENV PYTHONUNBUFFERED=1

# 4. 复制依赖清单到容器
COPY requirements.txt .

# 5. 安装依赖 (关键点：使用了清华源 -i 参数)
# --no-cache-dir 可以减小镜像体积
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 6. 复制具体的代码文件
COPY main.py .

# 7. 容器启动时的默认命令
CMD ["python", "main.py"]
