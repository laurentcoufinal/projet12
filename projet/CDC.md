Vous êtes architecte logiciel, spécialisé en systèmes distribués, dans l’entreprise DonnÉlite, une scale-up française spécialisée dans l’analyse de données en temps réel pour des clients grands comptes (retail, finance, logistique)..

 

L’entreprise est en forte croissance depuis 18 mois. Sa plateforme permet de collecter, traiter et exploiter des flux de données massifs pour produire des indicateurs décisionnels et alimenter des modèles prédictifs.

Cependant, cette croissance rapide met aujourd’hui en difficulté le système d’information. Plusieurs incidents récents ont mis en évidence des limites importantes en termes de performance, de maintenabilité et de capacité à intégrer de nouvelles solutions.

 

DonnÉlite souhaite faire évoluer son système d’information pour répondre aux enjeux de montée en charge, de fiabilité et d’évolutivité, tout en intégrant de nouveaux composants techniques nécessaires à ses activités data et IA.

Vous êtes chargé de :

analyser le système existant dans sa globalité ;

identifier les limites techniques et fonctionnelles actuelles ;

​évaluer des solutions techniques adaptées puis en sélectionner un ou des comme nouveau.x composant.s ;

proposer une nouvelle architecture adaptée aux enjeux de croissance ;

définir les modalités d’intégration des nouveaux composants sélectionnés.

Vous recevez un mail de Karyma, Head of Engineering, qui détaille les enjeux de la mission :

 

De : Karyma
À : Vous
Sujet : Évolution de notre SI

Bonjour,

Comme tu as pu le constater, notre plateforme commence à atteindre ses limites. Nous avons grandi plus vite que prévu, et certaines décisions techniques prises au départ ne sont plus adaptées à notre volume actuel de données ni à nos nouveaux cas d’usage.

Aujourd’hui, notre système repose sur un ensemble de services hétérogènes, avec des dépendances fortes et une architecture qui manque de lisibilité. Cela complique autant la maintenance que l’évolution de la plateforme.

Nous devons prendre du recul et structurer une nouvelle vision d’architecture. Pour cela, je te partage un premier ensemble de documents internes :

un rapport technique interne décrivant l’état actuel du système (partiel et possiblement obsolète sur certains points) ;

un document interne présentant le contexte métier et l'évolution fonctionnelle de la plateforme ;
un dossier interne regroupant des échanges, incidents et retours utilisateurs issus des différentes équipes.

Ces documents ne sont pas complets, mais ils doivent te permettre de reconstruire une compréhension du système existant.

 

J’aimerais que tu travailles sur trois éléments clés : 

Dans un premier temps, j’ai besoin d’un rapport d’audit du système existant qui inclut :

une modélisation du système existant, 

une analyse fonctionnelle et technique, 

une étude des dépendances entre composants, 

une évaluation des risques et contraintes, 

une analyse des impacts écologique et numérique,

une synthèse des besoins d’évolution du SI.

Ensuite, sur la base de cet audit, je souhaite que tu proposes une nouvelle architecture du système cible. Cette architecture devra inclure des composants du système existant ainsi que des nouveaux composants techniques. Tu formaliseras cela dans un document de définition d’architecture qui présente : 

la modélisation du domaine métier,

la description de l’architecture du système cible,

un benchmark de solutions techniques pour répondre aux limites identifiées,

la justification des solutions techniques et architecturales retenues en t’appuyant sur ton benchmark technologique, 

les interactions entre les composants de l’architecture du système cible, y compris les composants actuels et les nouveaux composants retenus,

les critères d’acceptation permettant de valider la solution identifiée.

Enfin, nous devons anticiper l’intégration des nouveaux composants dans notre écosystème, comme les solutions techniques que tu auras déjà retenues dans ta définition d’architecture. Pour cela, je te demande de produire un dossier d’intégration qui détaille : 

l’analyse de compatibilité entre (au moins un) des nouveaux composants que tu proposes et des composants du SI actuels, 

les modalités de configuration et d’intégration du ou des composants dans le système existant, 

la définition et la documentation d’onboarding pour l’environnement de développement que nos équipes pourront suivre afin de le mettre en place.

L’objectif n’est pas (uniquement) de proposer une architecture théorique, mais bien une solution cohérente, justifiée et intégrable dans notre contexte actuel.

Je compte sur toi pour challenger l’existant et expliciter tes hypothèses.

Nous ferons un point ensemble une fois que tu auras avancé sur ces éléments.

 

Karyma
Head of Engineering
DonnÉlite

Pièces jointes : 

rapport technique de l’architecture actuelle du système

document interne de présentation métier et d'évolution fonctionnelle de la plateforme
dossier interne des échanges, incidents et retours utilisateurs

C’est à vous de proposer une architecture cible capable de répondre aux enjeux actuels et futurs de DonnÉlite !

Pour préparer l’évolution du système d’information de DonnÉlite, commencez par analyser le fonctionnement actuel de la plateforme. À partir des documents techniques, des incidents, des retours utilisateurs et des échanges internes fournis dans le starter kit, reconstituez une vision exploitable du système existant.

Vous allez analyser :

les composants principaux ;

les flux de données ;

les dépendances techniques ;

les contraintes organisationnelles ;

les limites liées à la montée en charge.

Évaluez également les risques techniques, les impacts métier et les impacts écologique et numérique du système actuel.

Enfin, formalisez une synthèse d’audit permettant d’identifier les principaux besoins d’évolution du SI.

 

Prérequis

Avoir pris connaissance : 

de la demande de Karyma, head of Engineering,

des documents techniques et organisationnels fournis.

Résultat attendu

Une V1 complète du rapport d’audit contenant :

une modélisation exploitable du système actuel ;

une analyse fonctionnelle et technique ;

une étude des dépendances entre composants ;

une évaluation des risques et contraintes ;

une analyse des impacts, notamment en terms écologique et numérique ;

une synthèse des besoins d’évolution du SI.

Recommandations

Commencez par analyser le système actuel dans sa globalité afin d’en comprendre le fonctionnement, les composants et les usages métier.

Reconstituer progressivement le fonctionnement réel du système

Formaliser votre compréhension sous forme de schémas et de descriptions

Identifier les composants critiques et leurs responsabilités

Cartographier les principaux flux de données et les interactions entre services

Prioriser les problèmes observés selon leur impact métier et technique

Distinguer clairement :

entre les aspects fonctionnels des aspects techniques

entre les constats, les hypothèses et les besoins d’évolution

Suivre le cours OpenClassrooms recommandé dans ce projet, Appliquez les principes du Green IT dans votre entreprise, pour savoir analyser l’impact écologique.
Outils

Draw.io ou Miro pour la cartographie du système

Markdown ou tout éditeur de documentation

Outils de modélisation UML / C4

Tableau de priorisation ou matrice de risques

Points de vigilance

Ne pas proposer immédiatement une nouvelle architecture, ni transformer le rapport d’audit en document de solution à ce stade.

Faire attention aux contradictions présentes dans les documents fournis

Ne pas sous-estimer les contraintes historiques du SI

Expliciter les hypothèses et les limites de votre analyse lorsque certaines informations sont manquantes

Pour finaliser la conception du SI renouvelé, anticipez l’intégration des nouveaux composants dans le système existant. 

Définissez les modalités d’intégration et les étapes de configuration de ce composant dans le système cible.

Enfin, préparez l’environnement de développement et documentez l’onboarding nécessaire pour les équipes techniques à le mettre en place.

 

Prérequis

Avoir : 

défini l’architecture cible

modélisé les interactions

identifié les composants à intégrer

Résultat attendu

Le dossier d’intégration complet contenant :

une stratégie d’intégration et de configuration d’un nouveau composant dans le système existant ;

la définition et la documentation d’onboarding pour l’environnement de développement

Les versions finales des deux autres livrables rédigés, tout en harmonisation.

Recommandations

Identifier des composants (= nouvelles solutions technologiques) à intégrer dans le SI (à partir de la définition d’architecture).

Sélectionnez (au moins) un nouveau composant des solutions techniques que vous avez choisies lors de l’étape précédente.

Focalisez-vous sur ce composant et définissez les contraintes d’intégration.

Identifier les impacts d’intégration sur le SI actuel

Détailler les contraintes techniques du composant choisi.

Définir les étapes de configuration et de paramétrage

Structurer un environnement de développement cohérent et reproductible par les équipes

Simplifier l’onboarding technique en prenant en compte les équipes techniques

Elles devront mettre en place l’environnement de développement sans votre aide directe.

Outils

GitHub ou GitLab pour structurer l’environnement de développement

Documentation technique

Points de vigilance

Ne pas sous-estimer la complexité ni les contraintes d’intégration.

Ne pas négliger l’expérience des développeurs.