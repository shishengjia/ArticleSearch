import json
from django.shortcuts import render
from django.views.generic.base import View
from search.models import ArticleType
from django.http import HttpResponse
from elasticsearch import Elasticsearch
from datetime import datetime
import redis

client = Elasticsearch(hosts=["127.0.0.1"])
redis_cli = redis.StrictRedis()


class IndexView(View):
    def get(self, request):
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        topn_search = [item.decode("utf-8") for item in topn_search]
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    """
    返回搜索建议
    """
    def get(self, request):
        key_words = request.GET.get('s', '')
        re_datas = []
        if key_words:
            s = ArticleType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest",
                "fuzzy": {
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                re_datas.append(source["title"])
        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    """
    搜索结果
    """
    def get(self, request):
        key_words = request.GET.get("q", "")

        # 搜索关键词 搜索数量
        redis_cli.zincrby("search_keywords_set", 1, key_words)

        # 获取 topn 搜索
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        topn_search = [item.decode("utf-8") for item in topn_search]
        # 页码
        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except:
            page = 1

        # 获取文章总数量
        jobbole_count = redis_cli.get("jobbole_count")

        # 计算查询时间
        start_time = datetime.now()
        response = client.search(
            index="jobbole",
            body={
                "query": {
                    "multi_match": {
                        "query": key_words,
                        "fields": ["tags", "title", "content"]
                    }
                },
                "from": (page-1)*6,
                "size": 6,
                "highlight": {
                    "pre_tags": ['<span class="keyWord">'],
                    "post_tags": ['</span>'],
                    "fields": {
                        "title": {},
                        "content": {},
                    }
                }
            }
        )

        # 查询结束时间
        end_time = datetime.now()
        last_seconds = (end_time-start_time).total_seconds()

        # match的所有文章
        total_nums = response["hits"]["total"]
        if (page % 6) > 0:
            page_nums = int(total_nums/6) + 1
        else:
            page_nums = int(total_nums/6)
        hit_list = []
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            # 如果在高亮词里，从高亮词里选取
            if "title" in hit["highlight"]:
                hit_dict["title"] = "".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "content" in hit["highlight"]:
                hit_dict["content"] = "".join(hit["highlight"]["content"])[:500]
            else:
                hit_dict["content"] = hit["_source"]["content"][:500]

            hit_dict["create_date"] = hit["_source"]["create_date"]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]

            hit_list.append(hit_dict)

        return render(request, "result.html", {"page": page,
                                               "all_hits": hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums": page_nums,
                                               "last_seconds": last_seconds,
                                               "jobbole_count": jobbole_count,
                                               "topn_search": topn_search})
