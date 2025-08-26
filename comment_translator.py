#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop代码注释中英文转换工具
此脚本用于将Java代码中的英文注释转换为中文注释
"""
import os
import re
import argparse
from typing import List, Tuple, Optional

class CommentTranslator:
    """代码注释翻译器"""
    
    def __init__(self):
        """初始化翻译器"""
        # 预定义的常用术语翻译映射
        self.term_translations = {
            'NameNode': '名称节点',
            'DataNode': '数据节点',
            'HDFS': 'HDFS（Hadoop分布式文件系统）',
            'YARN': 'YARN（资源管理器）',
            'MapReduce': 'MapReduce（分布式计算框架）',
            'block': '数据块',
            'blockManager': '块管理器',
            'namespace': '命名空间',
            'replication': '副本',
            'checkpoint': '检查点',
            'edits': '编辑日志',
            'fsimage': '文件系统镜像',
            'journal': '日志',
            'journalnode': '日志节点',
            'RPC': 'RPC（远程过程调用）',
            'HTTP': 'HTTP（超文本传输协议）',
            'configuration': '配置',
            'client': '客户端',
            'server': '服务器',
            'daemon': '守护进程',
            'thread': '线程',
            'lock': '锁',
            'cache': '缓存',
            'metric': '指标',
            'balancer': '均衡器',
            'disk': '磁盘',
            'volume': '卷',
            'directory': '目录',
            'file': '文件',
            'path': '路径',
            'exception': '异常',
            'listener': '监听器',
            'buffer': '缓冲区',
            'stream': '流',
            'socket': '套接字',
            'channel': '通道',
            'memory': '内存',
            'storage': '存储',
            'persistence': '持久化',
            'serialization': '序列化',
            'deserialization': '反序列化',
            'protocol': '协议',
            'interface': '接口',
            'class': '类',
            'method': '方法',
            'parameter': '参数',
            'return': '返回',
            'value': '值',
            'field': '字段',
            'variable': '变量',
            'object': '对象',
            'instance': '实例',
            'static': '静态',
            'final': '最终',
            'private': '私有',
            'public': '公共',
            'protected': '受保护',
            'package': '包',
            'import': '导入',
            'try': '尝试',
            'catch': '捕获',
            'finally': '最终',
            'throw': '抛出',
            'throws': '抛出',
            'synchronized': '同步',
            'volatile': '易变',
            'transient': '瞬态',
            'native': '本地',
            'abstract': '抽象',
            'extends': '继承',
            'implements': '实现',
            'override': '重写',
            'annotation': '注解',
            'lambda': 'Lambda表达式',
            'thread pool': '线程池',
            'executor': '执行器',
            'future': 'Future对象',
            'callback': '回调',
            'listener': '监听器',
            'observer': '观察者',
            'subject': '主题',
            'factory': '工厂',
            'builder': '构建器',
            'singleton': '单例',
            'proxy': '代理',
            'decorator': '装饰器',
            'adapter': '适配器',
            'strategy': '策略',
            'template': '模板',
            'command': '命令',
            'state': '状态',
            'observer': '观察者',
            'iterator': '迭代器',
            'factory method': '工厂方法',
            'abstract factory': '抽象工厂',
            'builder pattern': '构建器模式',
            'singleton pattern': '单例模式',
            'adapter pattern': '适配器模式',
            'bridge pattern': '桥接模式',
            'composite pattern': '组合模式',
            'decorator pattern': '装饰器模式',
            'facade pattern': '外观模式',
            'flyweight pattern': '享元模式',
            'proxy pattern': '代理模式',
            'chain of responsibility': '责任链模式',
            'command pattern': '命令模式',
            'interpreter pattern': '解释器模式',
            'iterator pattern': '迭代器模式',
            'mediator pattern': '中介者模式',
            'memento pattern': '备忘录模式',
            'observer pattern': '观察者模式',
            'state pattern': '状态模式',
            'strategy pattern': '策略模式',
            'template method': '模板方法',
            'visitor pattern': '访问者模式',
        }
        
        # 注释正则表达式
        self.javadoc_pattern = re.compile(r'(/\*\*.*?\*/)', re.DOTALL)
        self.block_comment_pattern = re.compile(r'(/\*.*?\*/)', re.DOTALL)
        self.line_comment_pattern = re.compile(r'(//.*)$', re.MULTILINE)
        self.string_pattern = re.compile(r'"(?:[^"\\]|\\.)*"', re.DOTALL)
        self.char_pattern = re.compile(r'\'(?:[^\'\\]|\\.)*\'', re.DOTALL)
        
    def translate_term(self, text: str) -> str:
        """翻译文本中的技术术语"""
        for term, translation in self.term_translations.items():
            # 使用边界词匹配，避免部分匹配
            text = re.sub(rf'\b{re.escape(term)}\b', translation, text, flags=re.IGNORECASE)
        return text
    
    def translate_comment(self, comment: str) -> str:
        """翻译单个注释块"""
        # 保留注释标记，只翻译内容部分
        if comment.startswith('/**'):
            # Javadoc注释
            content = comment[3:-2].strip()  # 去掉 /** 和 */
            content = self.translate_term(content)
            # 简单的句子翻译（这里只是示例，实际可能需要更复杂的处理或API调用）
            return f"/**\n * {content}\n */" if '\n' in content else f"/** {content} */"
        elif comment.startswith('/*'):
            # 块注释
            content = comment[2:-2].strip()  # 去掉 /* 和 */
            content = self.translate_term(content)
            return f"/* {content} */"
        elif comment.startswith('//'):
            # 行注释
            content = comment[2:].strip()  # 去掉 //
            content = self.translate_term(content)
            return f"// {content}"
        return comment
    
    def is_english_text(self, text: str) -> bool:
        """判断文本是否主要是英文"""
        # 简单判断：检查文本中是否包含较多的英文字母
        english_chars = sum(1 for c in text if c.isalpha() and ord(c) < 128)
        total_chars = max(1, sum(1 for c in text if c.isalpha()))  # 避免除以0
        return english_chars / total_chars > 0.5
    
    def process_file(self, file_path: str) -> bool:
        """处理单个Java文件"""
        if not file_path.endswith('.java'):
            return False
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    content = f.read()
            except Exception as e:
                print(f"无法读取文件 {file_path}: {e}")
                return False
        
        # 记录字符串和字符常量位置，避免替换它们
        strings = []
        chars = []
        
        # 保存字符串常量
        for match in self.string_pattern.finditer(content):
            strings.append((match.start(), match.end(), match.group()))
        
        # 保存字符常量
        for match in self.char_pattern.finditer(content):
            chars.append((match.start(), match.end(), match.group()))
        
        # 替换注释
        def replace_comment(match):
            comment = match.group(1)
            # 检查注释是否在字符串或字符常量中
            start, end = match.span(1)
            for s_start, s_end, _ in strings + chars:
                if s_start <= start and end <= s_end:
                    return comment
            # 如果注释主要是英文，则进行翻译
            if self.is_english_text(comment):
                return self.translate_comment(comment)
            return comment
        
        # 先替换Javadoc注释
        new_content = self.javadoc_pattern.sub(replace_comment, content)
        # 替换块注释
        new_content = self.block_comment_pattern.sub(replace_comment, new_content)
        # 替换行注释
        new_content = self.line_comment_pattern.sub(replace_comment, new_content)
        
        # 写回文件
        if new_content != content:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"已更新文件: {file_path}")
                return True
            except Exception as e:
                print(f"无法写入文件 {file_path}: {e}")
                return False
        return False
    
    def process_directory(self, dir_path: str) -> Tuple[int, int]:
        """处理目录下的所有Java文件"""
        total_files = 0
        updated_files = 0
        
        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.endswith('.java'):
                    total_files += 1
                    file_path = os.path.join(root, file)
                    if self.process_file(file_path):
                        updated_files += 1
        
        return total_files, updated_files

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Hadoop代码注释中英文转换工具')
    parser.add_argument('path', help='要处理的文件或目录路径')
    parser.add_argument('--dry-run', action='store_true', help='仅显示需要更新的文件，不实际修改')
    
    args = parser.parse_args()
    
    translator = CommentTranslator()
    
    if os.path.isfile(args.path):
        if args.dry_run:
            try:
                with open(args.path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # 简单检查是否有英文注释需要翻译
                has_english_comments = re.search(r'(/\*\*.*?[a-zA-Z].*?\*/|/\*.*?[a-zA-Z].*?\*/|//.*?[a-zA-Z])', content, re.DOTALL)
                if has_english_comments:
                    print(f"需要更新文件: {args.path}")
                else:
                    print(f"无需更新文件: {args.path}")
            except Exception as e:
                print(f"无法读取文件 {args.path}: {e}")
        else:
            translator.process_file(args.path)
    elif os.path.isdir(args.path):
        if args.dry_run:
            print(f"扫描目录: {args.path}")
            # 模拟扫描过程
            for root, _, files in os.walk(args.path):
                for file in files:
                    if file.endswith('.java'):
                        file_path = os.path.join(root, file)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            has_english_comments = re.search(r'(/\*\*.*?[a-zA-Z].*?\*/|/\*.*?[a-zA-Z].*?\*/|//.*?[a-zA-Z])', content, re.DOTALL)
                            if has_english_comments:
                                print(f"需要更新文件: {file_path}")
                        except Exception as e:
                            print(f"无法读取文件 {file_path}: {e}")
        else:
            total, updated = translator.process_directory(args.path)
            print(f"处理完成: 共 {total} 个Java文件, 更新了 {updated} 个文件")
    else:
        print(f"路径不存在: {args.path}")

if __name__ == '__main__':
    main()