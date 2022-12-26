Vocal <- read.table('Vocal.csv', header = T, sep=";", fileEncoding="Windows-1251")


library(ggplot2)

subset(Vocal, Вокализация = писки )

order(Vocal$Месяц)


ggplot(Vocal, aes(factor(Mounth), col = Vocalization, fill = Vocalization))+
  geom_bar()+
  xlab('Месяц')+
  ylab('Вокализация')


ggplot(Vocal, aes(factor(Mounth)))+
  geom_bar(fill="slateblue", alpha=0.8)+
  xlab('Месяц')+
  ylab('') +
  facet_wrap(~ Vocalization)+
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Активность вокализации по месяцам")


data <- read.table('Vocalization.csv', header = T, sep=";")
normality_test(data)


t.test(Low_f ~ Type, data)

wilcox.test(data[, 7] ~ Type, data)

boxplot(H_f ~ Type, data = data)

ggplot(data, aes(Type, H_f))+
  geom_boxplot(fill="slateblue", alpha=0.2)+
  xlab('Тип вокализации')+
  ylab('Частота, кГц')+
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Максимальная частота разных типов вокализаций")

  
ggplot(data, aes(Type, D_t))+
  geom_boxplot(fill="slateblue", alpha=0.2)+
  xlab('Тип вокализации')+
  ylab('Длительность, сек')+
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Длительность разных типов вокализаций")


ggplot(data, aes(Type, D_f))+
  geom_boxplot(fill="slateblue", alpha=0.2)+
  xlab('Тип вокализации')+
  ylab('Частота, кГц')+
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Глубина частотной модуляции разных типов вокализаций")


ggplot(data, aes(Type, Syl_num))+
  geom_boxplot(fill="slateblue", alpha=0.2)+
  xlab('Тип вокализации')+
  ylab('Количество силлаблов')+
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Количество силлаблов в разных типах вокализаций")


data <- read.table('finl.csv', header = T, sep=";", fileEncoding="Windows-1251")


Mounth.labs <- c("Январь", "Февраль", "Март", "Апрель", "Май", "Декабрь")
names(Mounth.labs) <- c(1, 2, 3, 4, 5, 12)


ggplot(data, aes(Num))+
  geom_bar(fill="slateblue", alpha=0.8)+
  facet_wrap(~ Mounth, labeller = labeller(Mounth = Mounth.labs))+
  xlab('Кол-во особей')+
  ylab('Кол-во встреч') +
  theme(plot.title = element_text(hjust = 0.5))+
  ggtitle("Размеры групп по месяцам")+
  scale_x_continuous(breaks = scales::pretty_breaks(n = 5))

 

citation(package = "ggplot2")
