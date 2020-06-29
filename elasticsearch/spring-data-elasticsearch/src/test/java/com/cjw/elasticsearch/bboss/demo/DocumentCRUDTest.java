package com.cjw.elasticsearch.bboss.demo;

import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/29 15:17
 * @since JDK1.8
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DocumentCRUDTest {
    @Autowired
    private BBossESStarter bbossESStarter;
    @Autowired
    DocumentCRUD documentCRUD;

    @Test
    public void testIndex() throws Exception {

        //删除/创建文档索引表
        documentCRUD.dropAndCreateAndGetIndex();

    }

    @Test
    public void testInsert() {
        //添加/修改单个文档
        documentCRUD.addAndUpdateDocument();
    }

    @Test
    public void testSearch() {
        //检索文档
        documentCRUD.search("searchDatas");
    }

}
