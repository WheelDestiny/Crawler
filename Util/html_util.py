#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 潘牛
'''
import urllib,mx.URL,threading,sys
sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from tld import get_fld, get_tld
class HtmlUtil:

    def __init__(self):
        self.lock = threading.Lock()

    def get_doc_charset(self,doc):
        #<meta charset="UTF-8" />
        #<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        charset = 'utf-8'
        meta = doc.xpath('//meta[@charset]')
        if meta and len(meta) > 0:
            charset = meta[0].attrib.get('charset',charset)
        else:
            meta = doc.xpath("//meta[@http-equiv='Content-Type']")
            if meta and len(meta) > 0:
                content = meta[0].attrib.get('content','')
                if content:
                    p = content.find('charset=')
                    if p > 0:
                        charset=content[p + len('charset='):]
        return charset

    def get_dom_parent_xpath(self,dom):
        parents = []
        p = dom
        while True:
            if p is None:
                break
            #print p.attrib
            parents.append(p)
            if p.attrib.get('id',None):
                break
            p = p.getparent()

        xpath = ['/']
        for p in reversed(parents):
            id_name = p.attrib.get('id',None)
            class_name = p.attrib.get('class',None)
            if id_name:
                xpath.append('/')
                xpath.append(p.tag)
                xpath.append('[@id=\'')
                xpath.append(id_name)
                xpath.append('\']')
            elif class_name:
                xpath.append('/')
                xpath.append(p.tag)
                xpath.append('[contains(@class,\'')
                xpath.append(class_name)
                xpath.append('\')]')
            else:
                xpath.append('/')
                xpath.append(p.tag)

        return "".join(xpath)

    def get_dom_parent_xpath_js(self,dom):
        parents = []
        p = dom
        while True:
            if p is None:
                break
            #print p.attrib
            parents.append(p)
            if p.get('id',None):
                break
            p = p.parent

        xpath = ['/']
        for p in reversed(parents):
            id_name = p.get('id',None)
            class_name = p.get('class',None)
            if id_name:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[@id=\'')
                xpath.append(id_name)
                xpath.append('\']')
            elif class_name:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[contains(@class,\'')
                xpath.append(class_name[0])
                xpath.append('\')]')
            else:
                xpath.append('/')
                xpath.append(p.name)

        return "".join(xpath)


    def get_dom_parent_xpath_js_new(self,dom):
        """
        带有标签序号的xpath
        :param dom: dom对象
        :return: xpath字符串
        """
        parents = []
        #同级当前标签的序号列表
        index_nums = []
        #标签的名称列表
        tag_names = []
        p = dom
        while True:
            if p is None:
                break
            #print p.attrib

            parents.append(p)

            #计算同级标签下，当前标签在相同标签中所在的序号
            i = 1
            pre = p
            while True:
                pre = pre.previous_sibling
                if pre == None:
                    break;
                if pre.name == p.name:
                    i += 1
            index_nums.append(i)

            tag_names.append(p.name)

            if p.get('id',None):
                break

            p = p.parent

        print tag_names
        print index_nums
        xpath = ['/']
        for p in reversed(parents):
            id_name = p.get('id',None)
            class_name = p.get('class',None)
            index_num = index_nums.pop()
            if id_name:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[@id=\'')
                xpath.append(id_name)
                xpath.append('\']')

            #elif class_name:
            #    xpath.append('/')
            #    xpath.append(p.name)
            #    xpath.append('[contains(@class,\'')
            #    xpath.append(class_name[0])
            #    xpath.append('\')]')
            elif index_num >= 1:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[')
                xpath.append(str(index_num))
                xpath.append(']')
            else:
                xpath.append('/')
                xpath.append(p.name)

        return "".join(xpath)

    def get_url_host(self, url):
        s1 = urllib.splittype(url)[1]
        return urllib.splithost(s1)[0]

    def get_url_domain(self,url):
        """
        获取url的domain
        """
        # 加锁
        self.lock.acquire()
        domain = get_fld(url)
        #释放锁
        self.lock.release()
        return domain

    def get_format_url(self, url, a_doc, host):
        '''
        格式化a链接的url + 网页链接归一化处理

        :param url:    种子url
        :param a_doc:  a 链接的dom对象
        :param host:   种子url的host
        :return:
        '''
        a_href = a_doc.get('href')
        #a_href = "javascript:void(0)"
        try:
            if a_href is not None and a_href.__len__() > 0:
                a_href = str(a_href).strip()
                # http://www.baidu.com#aaa  --> http://www.baidu.com
                a_href = a_href[:a_href.index('#')] if a_href.__contains__('#') else a_href
                # a_href = a_href.encode('utf8')
                # a_href = urllib.quote(a_href,safe='.:/?&=')
                if a_href.startswith('//'):
                    url = 'https:' + a_href if url.startswith('https:') else 'http:' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('/'):
                    url = 'https://' + host + a_href if url.startswith('https:') else 'http://' + host + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('./') or a_href.startswith('../'):
                    url = mx.URL.URL(str(url) + '/' + a_href)
                    a_href = url.url
                elif not a_href.startswith('javascript') and not a_href.startswith('mailto') and not a_href.startswith('http') and a_href != '':
                    url = 'https://' + host + '/' + a_href if url.startswith('https:') else 'http://' + host + '/' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                a_href = a_href[:-1] if a_href.endswith('/') else a_href
                #a_href = a_href.lower()
            # 验证a链接 href属性的有效性，如果是无效的，会抛出异常
            get_tld(a_href)
        except Exception, msg:
            # print "err==>%s" % msg
            # traceback.print_exc()
            return ''

        if not a_href.startswith('http'):
            return ''

        # 网页链接归一化
        if a_href.__contains__('?'):
            a_params_str = a_href[a_href.index('?') + 1:]
            a_params = a_params_str.split('&')
            a_params.sort()
            a_params_str = '&'.join(a_params)
            a_href = a_href[:a_href.index('?') + 1] + a_params_str

        return a_href


if __name__ == "__main__":

    hu = HtmlUtil()
    def task(arg):
        domain = hu.get_url_domain("https://www.hainiubl.com")
        print "%s |" % domain
    for i in range(10):
        t = threading.Thread(target=task,args=[i,])
        t.start()



