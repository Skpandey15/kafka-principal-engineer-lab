package com.kafkalab.eventconsole.producer.web;

import java.io.IOException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Rejects oversized request bodies up front, from the Content-Length header, before
 * a single byte is parsed. Bean validation caps the number and size of events, but it
 * runs AFTER the whole body has been read into memory -- this is what bounds that.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestSizeLimitFilter extends OncePerRequestFilter {

    static final long MAX_BODY_BYTES = 8L * 1024 * 1024;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long length = request.getContentLengthLong();
        if (length > MAX_BODY_BYTES) {
            response.setStatus(413);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Request body too large (max " + MAX_BODY_BYTES + " bytes)\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
