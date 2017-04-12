package it.unipi.acube.tagmebulk.servlet;

import java.lang.invoke.MethodHandles;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagmeBulkContextListener implements ServletContextListener {
	private final static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	public final static String TAGME_URL_PARAM = "it.unipi.di.acube.tagmebulk.tagme-url";
	public final static String TAGME_URL_DEFAULT = "https://tagme.d4science.org/tagme/tag";

	@Override
	public void contextInitialized(ServletContextEvent e) {
		LOG.info("Creating Smaph context.");
		ServletContext context = e.getServletContext();

		HttpClient client = HttpClientBuilder.create().setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
		        .build();

		context.setAttribute("tagme-url", context.getInitParameter(TAGME_URL_PARAM) != null
		        ? context.getInitParameter(TAGME_URL_PARAM) : TAGME_URL_DEFAULT);
		context.setAttribute("http-client", client);
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		LOG.info("Destroying Tagme-Bulk context.");
	}
}
