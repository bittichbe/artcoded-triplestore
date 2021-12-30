package tech.artcoded.triplestore.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class RealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>> {
  @Override
  public Collection<GrantedAuthority> convert(Jwt jwt) {
    if (jwt.hasClaim("realm_access")) {

      final Map<String, List<String>> realmAccess = jwt.getClaim("realm_access");
      return realmAccess.get("roles").stream()
                        .map(roleName -> "ROLE_" + roleName.toUpperCase())
                        .map(SimpleGrantedAuthority::new)
                        .collect(Collectors.toList());
    }
    return List.of();
  }
}
